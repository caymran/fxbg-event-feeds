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
    if status == 304:
        return []
    events = []
    chunks = body.split("BEGIN:VEVENT")
    for chunk in chunks[1:]:
        block = chunk.split("END:VEVENT")[0]
        lines = [l.strip() for l in block.splitlines() if l.strip()]
        def get(prefixes):
            if isinstance(prefixes, str):
                prefixes = [prefixes]
            for p in prefixes:
                for l in lines:
                    if l.startswith(p):
                        return l.split(':',1)[1]
            return None
        title = get(['SUMMARY'])
        loc = get(['LOCATION'])
        dtstart = get(['DTSTART;TZID=America/New_York','DTSTART'])
        dtend = get(['DTEND;TZID=America/New_York','DTEND'])
        def parse_ics_dt(s):
            if not s: return None
            s = s.replace('Z','')
            try:
                return parser.parse(s)
            except Exception:
                return None
        sdt = parse_ics_dt(dtstart)
        edt = parse_ics_dt(dtend)
        events.append({
            'title': title,
            'description': None,
            'link': None,
            'start': sdt.isoformat() if sdt else None,
            'end': edt.isoformat() if edt else None,
            'location': loc,
            'source': url,
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
    token = token_env or os.getenv("EVENTBRITE_TOKEN") or ""
    if not token:
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


def fetch_macaronikid_fxbg(days=30, user_agent="fxbg-event-bot/1.0"):
    """
    Scrapes Macaroni KID Fredericksburg:
      - crawls the calendar list pages
      - visits each event detail page
      - pulls the per-event "Add to Apple Calendar" .ics (preferred)
      - falls back to parsing the HTML when ICS isn't present
    Returns a list of normalized raw events (to be run through normalize_event).
    """
    import urllib.parse, re
    from bs4 import BeautifulSoup
    from datetime import datetime, timedelta
    from dateutil import tz

    base = "https://fredericksburg.macaronikid.com"
    start_url = f"{base}/events/calendar"
    collected = []
    seen_event_urls = set()

    def _get(url):
        if not robots_allowed(url, user_agent):
            return (403, "", {})
        status, body, headers = req_with_cache(url, headers={"User-Agent": user_agent}, throttle=(1,3))
        return status, body or "", headers

    # crawl up to ~days worth by following "next" pagination links (usually few pages)
    page_url = start_url
    pages_visited = 0
    max_pages = 8  # safety cap
    while page_url and pages_visited < max_pages:
        status, body, _ = _get(page_url)
        if status != 200 or not body:
            break
        soup = BeautifulSoup(body, "html.parser")

        # find event cards -> detail links (/events/{id}/...)
        for a in soup.select("a[href*='/events/']"):
            href = a.get("href") or ""
            if not re.search(r"/events/[0-9a-f]+", href):
                continue
            url = urllib.parse.urljoin(base, href.split("?")[0])
            seen_event_urls.add(url)

        # follow "next" pagination if present
        next_a = soup.select_one("a[rel='next'], a.pagination__link--next, a[aria-label='Next']")
        if next_a and next_a.get("href"):
            page_url = urllib.parse.urljoin(base, next_a["href"])
            pages_visited += 1
            jitter_sleep(0.3, 0.6)
        else:
            break

    # visit each event detail page, prefer the ICS link
    for ev_url in sorted(seen_event_urls):
        status, body, _ = _get(ev_url)
        if status != 200 or not body:
            continue
        soup = BeautifulSoup(body, "html.parser")

        # Try to find an "Add to Apple Calendar" / .ics link
        ics_href = None
        for a in soup.select("a[href]"):
            href = a.get("href") or ""
            text = (a.get_text(" ", strip=True) or "").lower()
            if href.lower().endswith(".ics") or "apple calendar" in text:
                ics_href = urllib.parse.urljoin(base, href)
                break

        # If we found an ICS, parse via existing fetch_ics()
        if ics_href:
            try:
                ics_events = fetch_ics(ics_href, user_agent=user_agent) or []
                for e in ics_events:
                    # normalize shape similar to other sources; prefer detail page URL as the "link"
                    e["link"] = ev_url
                    e["source"] = "macaronikid"
                    collected.append(e)
                jitter_sleep(0.25, 0.5)
                continue
            except Exception:
                pass  # fall back to HTML

        # Fallback: parse title / date / description from HTML
        title = None
        desc = None
        date_text = None
        loc = None

        # Common selectors on MacaroniKID
        h1 = soup.select_one("h1") or soup.select_one("[data-element='event-title']")
        if h1:
            title = h1.get_text(" ", strip=True)

        # date & time blob often near the title or in meta
        dt_nodes = soup.select("[data-element='event-date'], time, .event-date, .event-time")
        if dt_nodes:
            date_text = " ".join([n.get_text(" ", strip=True) for n in dt_nodes])

        # description
        dnode = soup.select_one("[data-element='event-description'], .article-content, .event-description")
        if dnode:
            desc = dnode.get_text(" ", strip=True)

        # location
        lnode = soup.select_one("[data-element='event-location'], .event-location, .location")
        if lnode:
            loc = lnode.get_text(" ", strip=True)

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
        jitter_sleep(0.25, 0.5)

    return collected
