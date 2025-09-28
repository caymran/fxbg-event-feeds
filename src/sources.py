import os, time, json, requests, feedparser, urllib.parse, re, hashlib, hashlib
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
    
    # ROBOTS_DEBUG
    import os
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

def fetch_html(url, css, user_agent="fxbg-event-bot/1.0", throttle=(2,5)):
    if not robots_allowed(url, user_agent):
        return []
    status, body, _ = req_with_cache(url, headers={"User-Agent": user_agent}, throttle=throttle)
    if status == 304:
        return []
    soup = BeautifulSoup(body, 'lxml')
    items = soup.select(css.get('item')) if css.get('item') else []
    out = []
    for el in items:
        # MACKID_ONLY_TIME: discard cards without a datetime/time
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
    # EVT_TOKEN_DEBUG
    import os, urllib.parse
    # Eventbrite fallback token handling
    import os, urllib.parse
    import os
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
            print(f"   Bandsintown ok: type={'list' if isinstance(data, list) else type(data).__name__}, count={len(data) if isinstance(data, list) else len(data.get('events', []))}")
        if os.getenv('FEEDS_DEBUG'):
            print(f"   Eventbrite ok: total keys={list(data.keys())}")
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
    import os
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
            print(f"   Eventbrite HTTP {status}")
            print((body or '')[:200])
        return []
    try:
        data = json.loads(body)
        if os.getenv('FEEDS_DEBUG'):
            print(f"   Bandsintown ok: type={'list' if isinstance(data, list) else type(data).__name__}, count={len(data) if isinstance(data, list) else len(data.get('events', []))}")
        if os.getenv('FEEDS_DEBUG'):
            print(f"   Eventbrite ok: total keys={list(data.keys())}")
    except Exception:
        return []
    out = []
    seq = data if isinstance(data, list) else data.get("events", [])
    for ev in seq:
        title = None
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
