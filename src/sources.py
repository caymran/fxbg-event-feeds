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
    
def fetch_macaronikid_fxbg_playwright(pages=12):
    """
    Browser-based crawler for Macaroni KID Fredericksburg using Playwright.
    Tries JS list pages; if none found, falls back to sitemap discovery.
    Returns raw events; main.py will normalize.
    """
    from playwright.sync_api import sync_playwright
    import os, re, urllib.parse, json
    from urllib.parse import urlsplit

    base = "https://fredericksburg.macaronikid.com"
    detail_pat = re.compile(r"^/events/[0-9a-f]{8,}(?:/[\w\-]*)?$", re.I)

    def _sitemap_discover_detail_urls():
        """Fallback: read robots.txt for Sitemap: lines, parse sitemaps, pick /events/<id> URLs."""
        urls = set()
        # robots.txt
        st, body, _ = req_with_cache(base + "/robots.txt", headers={"User-Agent": "Mozilla/5.0"})
        if st == 200 and body:
            maps = []
            for line in body.splitlines():
                line = line.strip()
                if line.lower().startswith("sitemap:"):
                    maps.append(line.split(":", 1)[1].strip())
            # Always try the common names too
            maps += [base + "/sitemap.xml", base + "/sitemap_index.xml", base + "/sitemap-events.xml"]
            seen = set()
            for sm in maps:
                if sm in seen:
                    continue
                seen.add(sm)
                st2, xml, _ = req_with_cache(sm, headers={"User-Agent": "Mozilla/5.0"}, throttle=(1,3))
                if st2 != 200 or not xml:
                    continue
                # very light xml parsing
                for m in re.finditer(r"<loc>\s*([^<]+)\s*</loc>", xml):
                    u = m.group(1).strip()
                    try:
                        path = urlsplit(u).path
                    except Exception:
                        path = u
                    if detail_pat.match(path):
                        urls.add(u)
        return urls

    out = []
    detail_urls = set()

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/125.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            timezone_id="America/New_York",
            viewport={"width": 1366, "height": 900},
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
        )
        page = ctx.new_page()

        # 1) Try list pages with JS
        starts = [f"{base}/events"] + [f"{base}/events?page={i}" for i in range(1, pages + 1)]
        for u in starts:
            try:
                page.goto(u, wait_until="networkidle", timeout=45000)
                # scroll to trigger lazy content
                for _ in range(3):
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    page.wait_for_timeout(400)
                # wait a bit for anchors to render
                try:
                    page.wait_for_selector("a[href*='/events/']", timeout=3000)
                except Exception:
                    pass
                hrefs = page.eval_on_selector_all(
                    "a[href*='/events/']",
                    "els => els.map(e => e.getAttribute('href'))"
                ) or []
                for h in hrefs:
                    if not h:
                        continue
                    absu = urllib.parse.urljoin(u, h.split("?", 1)[0])
                    path = urlsplit(absu).path
                    if detail_pat.match(path):
                        detail_urls.add(absu)
            except Exception:
                continue

        if os.getenv("FEEDS_DEBUG"):
            print(f"   MacKID (PW): detail_urls={len(detail_urls)} from JS pages")

        # 2) If JS didn’t yield anything, fall back to sitemaps
        if not detail_urls:
            sitemap_urls = _sitemap_discover_detail_urls()
            if os.getenv("FEEDS_DEBUG"):
                print(f"   MacKID (SITEMAP): detail_urls={len(sitemap_urls)}")
            detail_urls |= sitemap_urls

        # 3) Visit each detail; capture title/desc, then extract start/end from several sources
        for ev_url in sorted(detail_urls):
            try:
                page.goto(ev_url, wait_until="networkidle", timeout=45000)
                page.wait_for_timeout(500)

                # ---- Basic fields ----
                title = ""
                if page.locator("h1").count():
                    title = (page.inner_text("h1") or "").strip()

                desc = ""
                desc_sel = "[data-element='event-description'], .article-content, .event-description"
                if page.locator(desc_sel).count():
                    desc = (page.inner_text(desc_sel) or "").strip()

                sdt_str, edt_str = None, None  # ISO-ish strings; main.py normalize_event will parse

                # ---- (A) JSON-LD: Event objects (object OR list) ----
                texts = page.eval_on_selector_all(
                    "script[type='application/ld+json']",
                    "els => els.map(e => e.textContent)"
                ) or []
                for txt in texts:
                    try:
                        obj = json.loads(txt)
                        cands = obj if isinstance(obj, list) else [obj]
                        for d in cands:
                            if isinstance(d, dict) and d.get("@type") in ("Event", "Festival"):
                                sdt_str = sdt_str or d.get("startDate") or d.get("start_date")
                                edt_str = edt_str or d.get("endDate")   or d.get("end_date")
                                if not title:
                                    title = (d.get("name") or "").strip() or title
                    except Exception:
                        pass

                # ---- (B) <time datetime="..."> ----
                if page.locator("time[datetime]").count():
                    vals = []
                    for i in range(page.locator("time[datetime]").count()):
                        v = page.locator("time[datetime]").nth(i).get_attribute("datetime") or ""
                        if v: vals.append(v.strip())
                    if vals:
                        sdt_str = sdt_str or (vals[0] if vals else None)
                        edt_str = edt_str or (vals[1] if len(vals) > 1 else None)

                # ---- (C) meta itemprop start/end ----
                if not sdt_str:
                    sm = page.locator("meta[itemprop='startDate'], meta[itemprop='startdate']")
                    if sm.count():
                        v = sm.first.get_attribute("content") or ""
                        if v: sdt_str = v.strip()
                if not edt_str:
                    em = page.locator("meta[itemprop='endDate'], meta[itemprop='enddate']")
                    if em.count():
                        v = em.first.get_attribute("content") or ""
                        if v: edt_str = v.strip()

                # ---- (D) Google Calendar link (?dates=YYYYMMDDTHHMMSSZ/YYYYMMDDTHHMMSSZ) ----
                if not sdt_str:
                    links = page.eval_on_selector_all(
                        "a[href]",
                        "els => els.map(e => e.getAttribute('href'))"
                    ) or []
                    for href in links:
                        if not href:
                            continue
                        lower = href.lower()
                        if ("calendar.google.com/calendar" in lower) or ("google.com/calendar" in lower):
                            try:
                                from urllib.parse import urlsplit, parse_qs
                                qs = parse_qs(urlsplit(href).query)
                                if "dates" in qs and qs["dates"]:
                                    rng = qs["dates"][0]
                                    if "/" in rng:
                                        a, b = rng.split("/", 1)
                                        # Convert compact Google format to ISO-ish strings
                                        def _gcal_to_iso(s):
                                            s = s.strip()
                                            # e.g., 20251005T140000Z or 20251005
                                            if len(s) == 8:  # YYYYMMDD (all-day)
                                                return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
                                            if "T" in s:
                                                y, mo, d = s[0:4], s[4:6], s[6:8]
                                                hh, mm, ss = s[9:11], s[11:13], s[13:15] if len(s) >= 15 else ("00","00","00")
                                                return f"{y}-{mo}-{d}T{hh}:{mm}:{ss}Z" if s.endswith("Z") else f"{y}-{mo}-{d}T{hh}:{mm}:{ss}"
                                            return s
                                        sdt_str = sdt_str or _gcal_to_iso(a)
                                        edt_str = edt_str or _gcal_to_iso(b)
                                        break
                            except Exception:
                                pass

                # ---- (E) Apple Calendar (.ics) per-event ----
                # Now that robots allows macaronikid .ics, use it when present to enrich times/title.
                if True:
                    pairs = page.eval_on_selector_all(
                        "a[href]",
                        "els => els.map(e => [e.innerText, e.getAttribute('href')])"
                    ) or []
                    ics = None
                    for text, href in pairs:
                        if not href: 
                            continue
                        txt = (text or "").lower()
                        if href.lower().endswith(".ics") or "apple calendar" in txt:
                            ics = urllib.parse.urljoin(ev_url, href)
                            break
                    if ics:
                        try:
                            for e in fetch_ics(ics) or []:
                                if not title:
                                    title = (e.get("title") or "").strip() or title
                                sdt_str = sdt_str or e.get("start")
                                edt_str = edt_str or e.get("end")
                        except Exception:
                            pass

                # ---- (F) Visible date text fallback (lets parse_when help) ----
                if not (sdt_str or edt_str):
                    # Try common wrappers for date/time text
                    vals = []
                    for sel in ("[data-element='event-date']", ".event-date", ".event-time", ".date", ".time"):
                        if page.locator(sel).count():
                            for i in range(page.locator(sel).count()):
                                t = page.locator(sel).nth(i).inner_text().strip()
                                if t: vals.append(t)
                    date_text = " ".join(vals)
                    if date_text:
                        sdt, edt = parse_when(date_text, default_tz="America/New_York")
                        if sdt: sdt_str = sdt.isoformat()
                        if edt: edt_str = edt.isoformat()

                # Emit only if we have at least a title and some datetime
                if title and (sdt_str or edt_str):
                    out.append({
                        "title": title,
                        "description": desc,
                        "link": ev_url,
                        "start": sdt_str,
                        "end":   edt_str,
                        "location": None,
                        "source": "macaronikid",
                    })
                elif os.getenv("FEEDS_DEBUG"):
                    print("     · skipped (no datetime):", ev_url)

            except Exception as ex:
                if os.getenv("FEEDS_DEBUG"):
                    print("     · error detail:", ev_url, str(ex)[:120])
                continue

        ctx.close()
        browser.close()

    return out


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
        detail_pat = re.compile(r"^/events/[0-9a-f]{8,}(?:/[\w\-]*)?$", re.I)

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
