import os, time, json, requests, feedparser, urllib.parse, re, hashlib
from bs4 import BeautifulSoup
from dateutil import parser
from urllib.robotparser import RobotFileParser
from utils import parse_when, jitter_sleep

CACHE_PATH = "data/cache.json"

def load_cache():
    if os.path.exists(CACHE_PATH):
        try:
            return json.load(open(CACHE_PATH,'r',encoding='utf-8'))
        except Exception:
            return {"http_cache": {}}
    return {"http_cache": {}}

def save_cache(cache):
    os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
    json.dump(cache, open(CACHE_PATH,'w',encoding='utf-8'), indent=2)

def robots_allowed(url, user_agent="*"):
    # Allow-list explicit subscription feeds
    ALLOWLIST_SUBSTR = [
        '/common/modules/iCalendar/iCalendar.aspx',  # CivicEngage ICS
        '/calendar/1.xml',                           # UMW RSS
        '/events/?ical=1', '/events/feed'           # The Events Calendar common exports
    ]
    # Allow-list Macaroni KID per-event ICS
    try:
        host = urllib.parse.urlsplit(url).netloc.lower()
        if host.endswith("macaronikid.com") and url.lower().endswith(".ics"):
            return True
    except Exception:
        pass

    # Allow-list Eventbrite discovery and event detail pages
    try:
        parts = urllib.parse.urlsplit(url)
        if parts.netloc.endswith("eventbrite.com") and ("/d/" in parts.path or "/e/" in parts.path):
            return True
    except Exception:
        pass
    
    for sub in ALLOWLIST_SUBSTR:
        if sub in url:
            return True

    try:
        parts = urllib.parse.urlsplit(url)
        robots_url = f"{parts.scheme}://{parts.netloc}/robots.txt"
        rp = RobotFileParser()
        rp.set_url(robots_url)
        rp.read()
        allowed = rp.can_fetch(user_agent, url)
        if os.getenv('FEEDS_DEBUG') and not allowed:
            print(f"   robots.txt disallows: {url}")
        return allowed
    except Exception:
        return True

def _cache_key(url, headers):
    # include Authorization + User-Agent so cached bodies don't leak across creds
    h = headers or {}
    auth = h.get('Authorization','')
    ua = h.get('User-Agent','')
    return url + '||' + hashlib.sha1((auth+'|'+ua).encode('utf-8')).hexdigest()

def req_with_cache(url, headers=None, throttle=(2,5), max_retries=3):
    headers = headers or {}
    cache = load_cache()
    key = _cache_key(url, headers)
    entry = cache["http_cache"].get(key, {})
    if "etag" in entry:
        headers["If-None-Match"] = entry["etag"]
    if "last_modified" in entry:
        headers["If-Modified-Since"] = entry["last_modified"]

    session = requests.Session()
    backoff = 1
    for attempt in range(max_retries):
        try:
            resp = session.get(url, headers=headers, timeout=30)
            if resp.status_code == 304:
                body = entry.get("body", "")
                return 304, body, {}
            if resp.status_code in (200, 201):
                etag = resp.headers.get("ETag")
                lastmod = resp.headers.get("Last-Modified")
                body = resp.text
                cache["http_cache"][key] = {
                    "etag": etag,
                    "last_modified": lastmod,
                    "fetched_at": int(time.time()),
                    "body": body[:500000]
                }
                save_cache(cache)
                jitter_sleep(throttle[0], throttle[1])
                return resp.status_code, body, {"etag": etag, "last_modified": lastmod}
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            return resp.status_code, "", {}
        except requests.RequestException:
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
    return 599, "", {}

def fetch_thrillshare_ical(events_page_url, user_agent="fxbg-event-bot/1.0"):
    """
    Load a Thrillshare events page (e.g., https://gwes.fxbgschools.us/o/gwes/events),
    find the 'Click to Download Calendar' link to the generate_ical endpoint,
    then fetch & parse that ICS for all events.
    """
    if not robots_allowed(events_page_url, user_agent):
        return []
    status, body, _ = req_with_cache(events_page_url, headers={"User-Agent": user_agent}, throttle=(1,3))
    if status != 200 or not body:
        return []

    soup = BeautifulSoup(body, "html.parser")
    # Anchor points to .../api/v4/o/<org_id>/cms/events/generate_ical?... (provided by the page)
    a = soup.find("a", href=True, string=lambda s: s and "Download Calendar" in s)
    if not a:
        a = soup.select_one("a[href*='generate_ical']")
    if not a:
        return []

    ics_url = urllib.parse.urljoin(events_page_url, a["href"])
    events = fetch_ics(ics_url, user_agent=user_agent) or []
    # mark source/link
    for e in events:
        e["source"] = "thrillshare"
        e.setdefault("link", events_page_url)
    return events

# ---------------- Eventbrite HTML crawler (discovery/list → detail) ----------------

def _eb_clean_text(s: str) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", s).strip()

def _eb_location_str(place):
    """Build a single string for location from JSON-LD Place."""
    if not place:
        return ""
    name = ""
    addr_txt = ""
    if isinstance(place, dict):
        name = (place.get("name") or "").strip()
        addr = place.get("address")
        if isinstance(addr, dict):
            parts = [
                addr.get("streetAddress"),
                addr.get("addressLocality"),
                addr.get("addressRegion"),
                addr.get("postalCode"),
            ]
            addr_txt = " ".join([p.strip() for p in parts if p and str(p).strip()])
        elif isinstance(addr, str):
            addr_txt = addr.strip()
    elif isinstance(place, list):
        # take first
        return _eb_location_str(place[0])
    else:
        name = str(place).strip()
    if name and addr_txt:
        return f"{name} - {addr_txt}"
    return name or addr_txt

def _parse_eventbrite_detail(detail_url, user_agent="fxbg-event-bot/1.0"):
    """Parse a single Eventbrite event page; prefer JSON-LD."""
    headers = {"User-Agent": user_agent, "Accept-Language": "en-US,en;q=0.9"}
    st, body, _ = req_with_cache(detail_url, headers=headers, throttle=(1,3))
    if st != 200 or not body:
        return None

    soup = BeautifulSoup(body, "html.parser")

    # 1) JSON-LD @type=Event
    ev_name = desc = start = end = None
    location_str = ""

    for tag in soup.select('script[type="application/ld+json"]'):
        try:
            data = json.loads(tag.string or "")
        except Exception:
            continue

        def handle_evt(evt):
            nonlocal ev_name, desc, start, end, location_str
            nm  = _eb_clean_text(evt.get("name") or "")
            sdt = evt.get("startDate") or evt.get("start_date")
            edt = evt.get("endDate")   or evt.get("end_date")
            dsc = _eb_clean_text(evt.get("description") or "")
            loc = _eb_location_str(evt.get("location"))
            # Only take if it looks like a bona-fide event
            if nm and sdt:
                ev_name = ev_name or nm
                start   = start or sdt
                end     = end or edt
                desc    = desc or dsc
                location_str = location_str or loc

        if isinstance(data, dict):
            if data.get("@type") == "Event":
                handle_evt(data)
            # @graph can embed the Event
            for node in (data.get("@graph") or []):
                if isinstance(node, dict) and node.get("@type") == "Event":
                    handle_evt(node)
        elif isinstance(data, list):
            for node in data:
                if isinstance(node, dict) and node.get("@type") == "Event":
                    handle_evt(node)

    # 2) Fallbacks from visible HTML if JSON-LD was partial/missing
    if not ev_name:
        h = soup.select_one("h1, [data-testid='event-title']")
        if h:
            ev_name = _eb_clean_text(h.get_text(" ", strip=True))
    if not desc:
        about = soup.select_one("[data-testid='event-details'], section[data-spec='event-details']")
        if about:
            desc = _eb_clean_text(about.get_text(" ", strip=True))
    if not (start or end):
        # Pull the text near "Date and time"
        dt_blk = soup.find(lambda t: t.name in ("section","div") and "Date and time" in t.get_text(" ", strip=True))
        dt_txt = _eb_clean_text(dt_blk.get_text(" ", strip=True)) if dt_blk else ""
        # Try parse_when to split a range like "Sunday, Oct 19 · 11am - 4pm EDT"
        sdt, edt = parse_when(dt_txt or ev_name or "")
        if sdt:
            start = sdt.isoformat()
        if edt:
            end = edt.isoformat()

    if not location_str:
        loc_blk = soup.find(lambda t: t.name in ("section","div") and "Location" in t.get_text(" ", strip=True))
        if loc_blk:
            # Venue name often first strong/heading, address lines follow
            txt = [ln.strip() for ln in loc_blk.get_text("\n", strip=True).splitlines() if ln.strip()]
            if txt:
                # Heuristic: join first two lines with ' - ', include zip line if present
                if len(txt) >= 2:
                    location_str = _eb_clean_text(f"{txt[0]} - {' '.join(txt[1:])}")
                else:
                    location_str = _eb_clean_text(txt[0])

    if not (ev_name and start):
        return None

    evt = {
        "title": ev_name,
        "description": desc or "",
        "link": detail_url,
        "start": start,
        "end": end,
        "location": location_str or None,
        "source": "eventbrite",
    }
    if os.getenv('FEEDS_DEBUG'):
        t = (ev_name or "")[:60]
        loc_snip = (location_str or "")[:60]
        print(f"     · EB parsed: {t} | start:{start} end:{end} loc:{loc_snip}")
    return evt


def fetch_eventbrite_discovery(list_url, pages=10, user_agent="fxbg-event-bot/1.0"):
    """
    Crawl Eventbrite discovery pages like:
      https://www.eventbrite.com/d/va--fredericksburg/free--events/?page=1
    Collect event links and parse each detail page.

    'pages' is how many sequential pages to fetch starting from the 'page=' in list_url
    (default start = 1 if missing).
    """
    if not robots_allowed(list_url, user_agent):
        return []

    def _with_page(u: str, page_num: int) -> str:
        parts = list(urllib.parse.urlsplit(u))
        q = urllib.parse.parse_qs(parts[3])
        q["page"] = [str(page_num)]
        parts[3] = urllib.parse.urlencode({k: v[0] for k, v in q.items()})
        return urllib.parse.urlunsplit(parts)

    headers = {"User-Agent": user_agent, "Accept-Language": "en-US,en;q=0.9"}
    
    pages_seen = 0
    
    # starting page number
    try:
        qs = urllib.parse.parse_qs(urllib.parse.urlsplit(list_url).query)
        start_page = int((qs.get("page") or ["1"])[0])
    except Exception:
        start_page = 1

    detail_urls = set()

    # 1) Collect event detail links from discovery pages
    for i in range(start_page, start_page + int(pages)):
        u = _with_page(list_url, i)
        st, body, _ = req_with_cache(u, headers=headers, throttle=(1,3))
        if st != 200 or not body:
            continue
        pages_seen += 1
        soup = BeautifulSoup(body, "html.parser")

        # Event cards are anchors to /e/… tickets pages
        for a in soup.select("a[href*='/e/']"):
            href = (a.get("href") or "").split("?", 1)[0]
            if not href:
                continue
            try:
                absu = urllib.parse.urljoin(u, href)
                path = urllib.parse.urlsplit(absu).path
            except Exception:
                absu = href
                path = href
            # Filter to /e/… (exclude organizer/collection/etc.)
            if not path.startswith("/e/"):
                continue
            # Skip promo anchors
            if any(seg in path for seg in ("/organizer/", "/o/", "/collections/")):
                continue
            detail_urls.add(absu)

        if os.getenv('FEEDS_DEBUG'):
            print(f"   Eventbrite page {i}: found {len(detail_urls)} links (cumulative)")
        if os.getenv('FEEDS_DEBUG'):
            print(f"   Eventbrite (HTML): pages_visited={pages_seen} detail_urls={len(detail_urls)}")
    
    # 2) Visit each detail page and parse
    out = []
    for ev_url in sorted(detail_urls):
        ev = _parse_eventbrite_detail(ev_url, user_agent=user_agent)
        if ev:
            out.append(ev)
        elif os.getenv('FEEDS_DEBUG'):
            print("   · Eventbrite skipped (parse failed):", ev_url)

    return out

# -------------------------------------------------------------------------------

def fetch_rss(url, user_agent="fxbg-event-bot/1.0"):
    if not robots_allowed(url, user_agent):
        return []
    status, body, _ = req_with_cache(url, headers={"User-Agent": user_agent})
    if status == 304:
        return []
    feed = feedparser.parse(body)
    events = []
    for e in feed.entries:
        title = getattr(e, 'title', '').strip()
        desc = getattr(e, 'summary', '') or getattr(e, 'description', '')
        link = getattr(e, 'link', '')
        dt = None
        for k in ['start_time', 'published', 'updated', 'created']:
            if hasattr(e, k):
                try:
                    dt = parser.parse(getattr(e, k))
                    break
                except Exception:
                    pass
        events.append({
            'title': title,
            'description': desc,
            'link': link,
            'start': dt.isoformat() if dt else None,
            'end': None,
            'location': None,
            'source': url,
        })
    return events

def fetch_ics(url, user_agent="fxbg-event-bot/1.0"):
    if not robots_allowed(url, user_agent):
        return []
    status, body, _ = req_with_cache(url, headers={"User-Agent": user_agent})
    if status == 304 or status != 200 or not body:
        return []

    def ics_unescape(s: str) -> str:
        # RFC5545 escaping
        return (s.replace("\\n", "\n").replace("\\N", "\n")
                 .replace("\\,", ",").replace("\\;", ";")
                 .replace("\\\\", "\\"))

    def parse_ics_dt(s):
        if not s:
            return None
        s = s.replace('Z', '')
        try:
            return parser.parse(s)
        except Exception:
            return None

    events = []
    chunks = body.split("BEGIN:VEVENT")
    for chunk in chunks[1:]:
        block = chunk.split("END:VEVENT")[0]

        # Unfold lines: continuation lines start with space or tab
        raw_lines = block.splitlines()
        unfolded = []
        for ln in raw_lines:
            if ln.startswith((" ", "\t")) and unfolded:
                unfolded[-1] += ln[1:]
            else:
                unfolded.append(ln.rstrip("\r"))
        lines = [ln for ln in unfolded if ln.strip()]

        def get_prop(name: str):
            name_u = name.upper()
            for ln in lines:
                L = ln.upper()
                # Match NAME:... or NAME;PARAM=...:...
                if L.startswith(name_u + ":") or L.startswith(name_u + ";"):
                    return ln.split(":", 1)[1]
            return None

        title = get_prop("SUMMARY")
        loc   = get_prop("LOCATION")
        dtstart = get_prop("DTSTART")
        dtend   = get_prop("DTEND")
        desc    = get_prop("DESCRIPTION")
        url_prop = get_prop("URL")

        # Unescape textual fields
        if title: title = ics_unescape(title.strip())
        if loc:   loc   = ics_unescape(loc.strip())
        if desc:  desc  = ics_unescape(desc.strip())

        sdt = parse_ics_dt(dtstart)
        edt = parse_ics_dt(dtend)

        # Make URL absolute if feed used relative URL (CivicEngage often does)
        link = None
        if url_prop:
            try:
                link = urllib.parse.urljoin(url, url_prop.strip())
            except Exception:
                link = url_prop.strip()

        events.append({
            "title": title,
            "description": desc,
            "link": link,
            "start": sdt.isoformat() if sdt else None,
            "end":   edt.isoformat() if edt else None,
            "location": loc,
            "source": url,
        })

    return events


# ---------- Fredericksburg Free Press scraper ----------
from dateutil import parser as dtparse, tz

def _clean_text(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _parse_dt(val, default_tz="America/New_York"):
    if not val:
        return None
    try:
        dt = dtparse.parse(val)
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=tz.gettz(default_tz))
        return dt
    except Exception:
        return None

def fetch_freepress_calendar(url: str, default_tz="America/New_York"):
    """
    Scrape https://www.fredericksburgfreepress.com/calendar/ for events.
    Strategy:
      1) JSON-LD @type: Event
      2) Microdata itemtype=Event
      3) Fallback: common 'event card' selectors
    Returns list of dicts with keys: title, description, location, start, end, link, source
    """
    headers = {"User-Agent": "fxbg-event-feeds/1.0 (+github.com/caymran/fxbg-event-feeds)"}
    status, body, _ = req_with_cache(url, headers=headers, throttle=(2,5))
    if status == 304:
        return []
    if status != 200:
        if os.getenv('FEEDS_DEBUG'):
            print(f"   FreePress HTTP {status}")
        return []

    soup = BeautifulSoup(body, "html.parser")
    out = []

    # ---------- 1) JSON-LD Events ----------
    for tag in soup.select('script[type="application/ld+json"]'):
        try:
            data = json.loads(tag.string or "")
        except Exception:
            continue

        def emit(evt):
            name = _clean_text(evt.get("name", ""))
            if not name:
                return
            start = _parse_dt(evt.get("startDate"), default_tz)
            end   = _parse_dt(evt.get("endDate"),   default_tz)
            url_e = evt.get("url") or url
            desc  = _clean_text(evt.get("description", ""))

            loc_block = evt.get("location") or {}
            if isinstance(loc_block, dict):
                loc_name = loc_block.get("name") or ""
                addr = loc_block.get("address") or {}
                if isinstance(addr, dict):
                    addr_txt = " ".join(filter(None, [
                        addr.get("streetAddress"),
                        addr.get("addressLocality"),
                        addr.get("addressRegion"),
                        addr.get("postalCode")
                    ]))
                else:
                    addr_txt = addr if isinstance(addr, str) else ""
                location = _clean_text(" - ".join([loc_name, addr_txt]).strip(" -"))
            else:
                location = _clean_text(str(loc_block))

            out.append({
                "title": name,
                "description": desc,
                "location": location,
                "start": start.isoformat() if start else None,
                "end":   end.isoformat() if end else None,
                "link": url_e,
                "source": url,
            })

        # Plain object
        if isinstance(data, dict):
            if data.get("@type") == "Event":
                emit(data)
            # Graph / array embedded in dict
            for node in (data.get("@graph") or []):
                if isinstance(node, dict) and node.get("@type") == "Event":
                    emit(node)

        # Array of things
        if isinstance(data, list):
            for node in data:
                if isinstance(node, dict) and node.get("@type") == "Event":
                    emit(node)

    if out:
        return out

    # ---------- 2) Microdata Events ----------
    for ev in soup.select('[itemscope][itemtype*="schema.org/Event"], [itemscope][itemtype*="schema.org/event"]'):
        def gp(prop):
            el = ev.select_one(f'[itemprop="{prop}"]')
            if not el:
                return None
            # Prefer datetime attr if present
            if el.has_attr("content"):
                return el["content"]
            if el.has_attr("datetime"):
                return el["datetime"]
            return el.get_text(" ", strip=True)

        title = _clean_text(gp("name") or gp("summary") or "")
        start = _parse_dt(gp("startDate") or gp("startTime"), default_tz)
        end   = _parse_dt(gp("endDate")   or gp("endTime"),   default_tz)
        desc  = _clean_text(gp("description") or "")
        loc_name = ""
        loc_el = ev.select_one('[itemprop="location"]')
        if loc_el:
            nm = loc_el.select_one('[itemprop="name"]')
            if nm: loc_name = nm.get_text(" ", strip=True)
            if not loc_name:
                loc_name = loc_el.get_text(" ", strip=True)
        link_el = ev.select_one('a[href]')
        href = link_el['href'] if link_el and link_el.has_attr('href') else url

        if title:
            out.append({
                "title": title,
                "description": desc,
                "location": _clean_text(loc_name),
                "start": start.isoformat() if start else None,
                "end":   end.isoformat() if end else None,
                "link": href,
                "source": url,
            })

    if out:
        return out

    # ---------- 3) Fallback: common event-card patterns ----------
    candidates = soup.select(
        "article.type-tribe_events, "
        ".tribe-events-calendar-list__event, "
        "article.calendar-item, "
        "li.event, "
        "div.event, "
        "article"
    )
    for node in candidates:
        # Title
        a = node.select_one("a[href]")
        title = ""
        href = url
        if a:
            title = _clean_text(a.get_text(" ", strip=True))
            href = a.get("href") or href
        if not title:
            h = node.select_one("h3, h2, .event-title")
            if h:
                title = _clean_text(h.get_text(" ", strip=True))
        # Date/Time text
        start_txt = None; end_txt = None
        tstarts = node.select('time[datetime]')
        if tstarts:
            start_txt = tstarts[0].get("datetime")
            if len(tstarts) > 1:
                end_txt = tstarts[1].get("datetime")
        if not start_txt:
            dt_guess = node.get_text(" ", strip=True)
            m = re.search(r"\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)?\.?,?\s*[A-Z][a-z]+\.?\s*\d{1,2}[^|,]*\d{1,2}:\d{2}\s*(?:AM|PM|am|pm)?", dt_guess)
            if m:
                start_txt = m.group(0)

        start = _parse_dt(start_txt, default_tz)
        end   = _parse_dt(end_txt,   default_tz)

        # Location
        loc = ""
        loc_el = node.select_one(".tribe-events-calendar-list__event-venue, .event-venue, .location")
        if loc_el:
            loc = _clean_text(loc_el.get_text(" ", strip=True))

        # Description snippet
        desc_el = node.select_one(".tribe-events-calendar-list__event-description, .entry-content, .event-description, p")
        desc = _clean_text(desc_el.get_text(" ", strip=True)) if desc_el else ""

        if title and start:
            out.append({
                "title": title,
                "description": desc,
                "location": loc,
                "start": start.isoformat(),
                "end":   end.isoformat() if end else None,
                "link": href,
                "source": url,
            })

    return out

# ---------- Generic HTML fetcher with optional parser hint ----------
def fetch_html(url, hints=None, user_agent="fxbg-event-bot/1.0", throttle=(2,5)):
    """
    When hints is a dict of CSS selectors (legacy behavior):
      hints = { 'item': '...', 'title': '...', 'date': '...', 'time': '...', 'location': '...', 'description': '...' }

    Or, you can pass a parser hint:
      hints = { 'parser': 'freepress', 'timezone': 'America/New_York' }
    """
    hints = hints or {}
    if not robots_allowed(url, user_agent):
        return []

    # Site-specific parser shortcut
    parser_name = (hints.get("parser") or "").lower()
    if "fredericksburgfreepress" in url or parser_name == "freepress":
        tzname = hints.get("timezone", "America/New_York")
        return fetch_freepress_calendar(url, default_tz=tzname)

    status, body, _ = req_with_cache(url, headers={"User-Agent": user_agent}, throttle=throttle)
    if status == 304:
        return []

    soup = BeautifulSoup(body, 'html.parser')
    out = []

    # Legacy CSS-based scraping path
    css = hints
    items = soup.select(css.get('item')) if css.get('item') else []
    for el in items:
        # discard cards without a datetime/time signal
        time_node = el.select_one("time[datetime]") or el.find("time", attrs={"datetime": True})
        maybe_text = (el.get_text(" ", strip=True) or '').lower()
        looks_time = bool(re.search(r'\b\d{1,2}(:\d{2})?\s*(a\.m\.|am|p\.m\.|pm)\b', maybe_text))
        if (not time_node) and (not looks_time):
            continue

        def pick(sel):
            if not sel: return None
            node = el.select_one(sel)
            return node.get_text(" ", strip=True) if node else None

        title = pick(css.get('title'))
        date_text = pick(css.get('date')) or pick(css.get('time'))
        loc = pick(css.get('location'))
        desc = pick(css.get('description'))
        s, e = parse_when(date_text)
        out.append({
            'title': title,
            'description': desc,
            'link': None,
            'start': s.isoformat() if s else None,
            'end': e.isoformat() if e else None,
            'location': loc,
            'source': url,
        })

    if not out:
        t = soup.select_one('h1, h2, .title')
        d = soup.select_one('time, .date, p')
        if t:
            s, e = parse_when(d.get_text(" ", strip=True) if d else None)
            out.append({
                'title': t.get_text(" ", strip=True),
                'description': (soup.select_one("body").get_text(" ", strip=True)[:500] if soup.select_one("body") else ""),
                'link': url,
                'start': s.isoformat() if s else None,
                'end': e.isoformat() if e else None,
                'location': None,
                'source': url,
            })

    return [e for e in out if e.get('title')]

def fetch_eventbrite(api_url, token_env=None):
    """
    Unified Eventbrite fetcher:
      - If `api_url` looks like an Eventbrite discovery or event HTML URL, use the HTML crawler.
      - Otherwise, treat as API endpoint and use token (existing behavior).
    """
    if re.search(r"//[^/]*eventbrite\.com/(d/|e/)", api_url):
        # HTML crawler path (no token required)
        if os.getenv('FEEDS_DEBUG'):
            print(f"→ Eventbrite discovery crawl: {api_url}")
        return fetch_eventbrite_discovery(api_url, pages=10)

    token = token_env or os.getenv("EVENTBRITE_TOKEN") or ""
    if not token:
        # No token and not a discovery URL -> nothing to do
        if os.getenv('FEEDS_DEBUG'):
            print("   Eventbrite API: missing token")
        return []
    headers = {"Authorization": f"Bearer {token}"}
    status, body, _ = req_with_cache(api_url, headers=headers, throttle=(2,5))
    if status == 304:
        return []
    if status != 200:
        if os.getenv('FEEDS_DEBUG'):
            print(f"   Eventbrite HTTP {status}")
            print((body or '')[:200])
        return []
    try:
        data = json.loads(body)
        if os.getenv('FEEDS_DEBUG'):
            print(f"   Eventbrite ok: top-level keys={list(data.keys())}")
    except Exception:
        return []
    out = []
    events = data.get("events") or data.get("data") or []
    for ev in events:
        title = (ev.get("name", {}) or {}).get("text") or ev.get("name")
        desc = (ev.get("description", {}) or {}).get("text") or ev.get("description")
        start = (ev.get("start") or {}).get("local") or ev.get("start")
        end = (ev.get("end") or {}).get("local") or ev.get("end")
        venue_name = None
        if ev.get("venue"):
            venue_name = ev["venue"].get("name")
        elif ev.get("venue_id"):
            venue_name = f"Venue ID {ev['venue_id']}"
        out.append({
            "title": title,
            "description": desc,
            "link": ev.get("url"),
            "start": start,
            "end": end,
            "location": venue_name,
            "source": "eventbrite"
        })
    return out

def fetch_bandsintown(url, app_id_env=None):
    if os.getenv('FEEDS_DEBUG'):
        print(f"   Bandsintown app_id present? {'YES' if (app_id_env or os.getenv('BANDSINTOWN_APP_ID')) else 'NO'}")
    app_id = app_id_env or os.getenv("BANDSINTOWN_APP_ID") or ""
    u = url.replace("${BANDSINTOWN_APP_ID}", app_id)
    if not app_id:
        if os.getenv('FEEDS_DEBUG'):
            print('   Bandsintown missing app_id (empty)')
        return []
    status, body, _ = req_with_cache(u, headers={"User-Agent": "fxbg-event-bot/1.0"})
    if status == 304:
        return []
    if status != 200:
        if os.getenv('FEEDS_DEBUG'):
            print(f"   Bandsintown HTTP {status}")
            print((body or '')[:200])
        return []
    try:
        data = json.loads(body)
        if os.getenv('FEEDS_DEBUG'):
            print(f"   Bandsintown ok: type={'list' if isinstance(data, list) else type(data).__name__}, count={len(data) if isinstance(data, list) else len(data.get('events', []))}")
    except Exception:
        return []
    out = []
    seq = data if isinstance(data, list) else data.get("events", [])
    for ev in seq:
        lineup = ev.get("lineup") or []
        if isinstance(lineup, list) and lineup:
            title = " / ".join(lineup) + " @ " + (ev.get("venue", {}).get("name") or "Unknown venue")
        else:
            title = (ev.get("title") or "Live music") + " @ " + (ev.get("venue", {}).get("name") or "Unknown venue")
        start = ev.get("starts_at") or ev.get("datetime") or ev.get("start")
        desc = ev.get("description") or ""
        venue = ev.get("venue", {})
        location = venue.get("name")
        link = ev.get("url") or ev.get("offer_url")
        out.append({
            "title": title,
            "description": desc,
            "link": link,
            "start": start,
            "end": None,
            "location": location,
            "source": "bandsintown"
        })
    return out

def _mackid_sitemap_discover_detail_urls(base_url, user_agent="Mozilla/5.0"):
    """
    Fallback for Macaroni KID: read robots.txt → find Sitemap: lines → fetch sitemaps
    → extract /events/... links (works even when list pages are JS-only).
    """
    from urllib.parse import urlsplit
    headers = {"User-Agent": user_agent, "Accept-Language": "en-US,en;q=0.9"}
    urls = set()

    # 1) robots.txt
    try:
        parts = urllib.parse.urlsplit(base_url)
        robots_url = f"{parts.scheme}://{parts.netloc}/robots.txt"
        st, body, _ = req_with_cache(robots_url, headers=headers, throttle=(1,3))
    except Exception:
        st, body = 0, ""

    sitemaps = []
    if st == 200 and body:
        for line in body.splitlines():
            line = line.strip()
            if line.lower().startswith("sitemap:"):
                sitemaps.append(line.split(":", 1)[1].strip())

    # Common sitemap locations (in case robots doesn’t list them)
    root = f"{parts.scheme}://{parts.netloc}"
    sitemaps += [f"{root}/sitemap.xml", f"{root}/sitemap_index.xml", f"{root}/sitemap-events.xml"]

    seen = set()
    detail_pat = re.compile(r"^/events/[A-Za-z0-9\-]{6,}(?:/[\w\-]*)?$", re.I)

    for sm in sitemaps:
        if sm in seen:
            continue
        seen.add(sm)
        st2, xml, _ = req_with_cache(sm, headers=headers, throttle=(1,3))
        if st2 != 200 or not xml:
            continue

        # Extract <loc>…</loc> values (loose XML parse)
        for m in re.finditer(r"<loc>\s*([^<]+)\s*</loc>", xml):
            u = m.group(1).strip()
            try:
                path = urlsplit(u).path
            except Exception:
                path = u
            if detail_pat.match(path):
                urls.add(u)

        # If this is a sitemap index, it may point to other sitemaps; follow shallowly
        for sm2 in re.finditer(r"<sitemap>\s*<loc>\s*([^<]+)\s*</loc>\s*</sitemap>", xml):
            u2 = sm2.group(1).strip()
            if u2 and u2 not in seen:
                seen.add(u2)
                st3, xml2, _ = req_with_cache(u2, headers=headers, throttle=(1,3))
                if st3 == 200 and xml2:
                    for m in re.finditer(r"<loc>\s*([^<]+)\s*</loc>", xml2):
                        u = m.group(1).strip()
                        try:
                            path = urlsplit(u).path
                        except Exception:
                            path = u
                        if detail_pat.match(path):
                            urls.add(u)

    return urls


def fetch_macaronikid_fxbg(days=60, user_agent=None):
    """
    Crawl Macaroni KID Fredericksburg:
      - list view (/events?page=1..)
      - backup: /events and /events/calendar
      - visit real event detail pages only
      - prefer per-event .ics; fallback to HTML dates
    Returns raw events to be normalized by normalize_event().
    """
    import os, urllib.parse, re, json
    from bs4 import BeautifulSoup
    from datetime import datetime, timedelta

    # Use a realistic browser UA; can be overridden with env MAC_KID_UA
    user_agent = user_agent or os.getenv("MAC_KID_UA") or (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    )

    base = "https://fredericksburg.macaronikid.com"
    start_urls = [
        f"{base}/events",
        f"{base}/events/calendar",
    ]
    # Add explicit pagination on list view (page numbers often work)
    for i in range(1, 9):  # crawl up to 8 pages
        start_urls.append(f"{base}/events?page={i}")

    def _get(url):
        # Intentionally skip robots for this public listing; some pages are over-restrictive.
        headers = {
            "User-Agent": user_agent,
            "Referer": base + "/",
            "Accept-Language": "en-US,en;q=0.9",
        }
        status, body, headers_out = req_with_cache(url, headers=headers, throttle=(1,3))
        return status, (body or ""), headers_out


    def _find_event_links(html, page_url):
        soup = BeautifulSoup(html, "html.parser")
        links = set()

        # Accept only true detail URLs, not listing/month pages.
        # Examples we accept:
        #   /events/681814d3ede0d566abf77b86
        #   /events/681814d3ede0d566abf77b86/some-slug
        # Reject:
        #   /events
        #   /events/calendar
        detail_pat = re.compile(r"^/events/[A-Za-z0-9\-]{6,}(?:/[\w\-]*)?$", re.I)

        for a in soup.select("a[href*='/events/']"):
            href = (a.get("href") or "").split("?")[0].strip()
            if not href:
                continue
            abs_url = urllib.parse.urljoin(page_url, href)
            # Normalize to path for regex
            try:
                from urllib.parse import urlsplit
                path = urlsplit(abs_url).path
            except Exception:
                path = href
            if detail_pat.match(path):
                links.add(abs_url)
        return links


    def _find_next_page(html, page_url):
        soup = BeautifulSoup(html, "html.parser")
        nxt = (
            soup.select_one("a[rel='next']") or
            soup.select_one("a.pagination__link--next") or
            soup.select_one("a[aria-label='Next']")
        )
        if nxt and nxt.get("href"):
            return urllib.parse.urljoin(page_url, nxt["href"])
        return None

    # Crawl all start URLs; list pages may paginate via ?page=
    detail_urls = set()
    pages_visited = 0
    max_pages = 20  # overall safety cap across all starts

    for start in start_urls:
        if pages_visited >= max_pages:
            break
        st, body, _ = _get(start)
        if st == 200 and body:
            new_links = _find_event_links(body, start)
            detail_urls |= new_links
            pages_visited += 1

    if os.getenv("FEEDS_DEBUG"):
        print(f"   MacKID: pages_visited={pages_visited} detail_urls={len(detail_urls)}")
        for u in list(sorted(detail_urls))[:5]:
            print("     · detail:", u)

    # If no links from list pages (JS-only?), fall back to sitemaps
    if not detail_urls:
        sitemap_links = _mackid_sitemap_discover_detail_urls(base, user_agent=user_agent)
        if os.getenv("FEEDS_DEBUG"):
            print(f"   MacKID (SITEMAP): detail_urls={len(sitemap_links)}")
        detail_urls |= sitemap_links


    collected = []

    for ev_url in sorted(detail_urls):
        st, body, _ = _get(ev_url)

        # Skip any list/calendar page that slipped through
        if ev_url.rstrip("/").endswith("/events") or ev_url.rstrip("/").endswith("/events/calendar"):
            continue

        if st != 200 or not body:
            continue
        soup = BeautifulSoup(body, "html.parser")

        # 1) Try per-event ICS link first (preferred)
        ics_href = None
        for a in soup.select("a[href]"):
            href = (a.get("href") or "").strip()
            txt = (a.get_text(" ", strip=True) or "").lower()
            if href.lower().endswith(".ics") or "apple calendar" in txt:
                ics_href = urllib.parse.urljoin(ev_url, href)
                break
        if ics_href:
            try:
                for e in fetch_ics(ics_href, user_agent=user_agent) or []:
                    e["source"] = "macaronikid"
                    e.setdefault("link", ev_url)
                    collected.append(e)
                continue
            except Exception:
                pass  # fall through to HTML fallback

        # 2) HTML fallback
        title = None
        desc = None
        loc = None
        date_text = None

        # titles can be in various wrappers
        h = soup.select_one("h1") or soup.select_one("[data-element='event-title']")
        if h:
            title = h.get_text(" ", strip=True)

        # description
        d = soup.select_one("[data-element='event-description'], .article-content, .event-description")
        if d:
            desc = d.get_text(" ", strip=True)

        # location
        l = soup.select_one("[data-element='event-location'], .event-location, .location, [itemprop='location']")
        if l:
            loc = l.get_text(" ", strip=True)

        # date/time: try structured elements, then any <time>, then strong/p elements with time-ish text

        # 1) Structured text blocks
        nodes = soup.select("[data-element='event-date'], .event-date, .event-time")
        if nodes:
            date_text = " ".join(n.get_text(" ", strip=True) for n in nodes if n.get_text(strip=True))

        # 2) ISO datetimes from <time datetime="...">
        iso_start, iso_end = None, None
        for t in soup.select("time[datetime]"):
            dtv = (t.get("datetime") or "").strip()
            if dtv:
                if not iso_start:
                    iso_start = dtv
                elif not iso_end:
                    iso_end = dtv
        if not date_text and (iso_start or iso_end):
            date_text = f"{iso_start or ''} {iso_end or ''}".strip()

        # 3) itemprop meta
        if not date_text:
            sm_tag = soup.select_one("meta[itemprop='startDate'], meta[itemprop='startdate']")
            em_tag = soup.select_one("meta[itemprop='endDate'], meta[itemprop='enddate']")
            sm = sm_tag.get("content").strip() if sm_tag and sm_tag.get("content") else ""
            em = em_tag.get("content").strip() if em_tag and em_tag.get("content") else ""
            if sm or em:
                date_text = f"{sm} {em}".strip()

        # 4) JSON-LD Event (object OR list)
        if not date_text:
            for s in soup.find_all("script", type="application/ld+json"):
                try:
                    dct = json.loads(s.string)
                    cand = [dct] if isinstance(dct, dict) else (dct if isinstance(dct, list) else [])
                    for obj in cand:
                        if isinstance(obj, dict) and obj.get("@type") in ("Event", "Festival"):
                            sm = obj.get("startDate") or obj.get("start_date") or ""
                            em = obj.get("endDate") or obj.get("end_date") or ""
                            if sm or em:
                                date_text = f"{sm} {em}".strip()
                                raise StopIteration
                except StopIteration:
                    break
                except Exception:
                    pass

        print("     · parsed:", (title or "")[:60], "| date_text:", (date_text or "")[:80])


        sdt, edt = parse_when(date_text or "", default_tz="America/New_York")
        collected.append({
            "title": title or "(untitled)",
            "description": desc or "",
            "link": ev_url,
            "start": sdt.isoformat() if sdt else None,
            "end": (edt.isoformat() if edt else None) if edt else None,
            "location": loc,
            "source": "macaronikid",
        })

    return collected
