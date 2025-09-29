import os, json, yaml, re
from datetime import datetime, timedelta, timezone
from dateutil import tz, parser
from ics import Calendar, Event
try:
    from ics.grammar.parse import ContentLine
except Exception:
    try:
        from ics.grammar.line import ContentLine
    except Exception:
        ContentLine = None
from sources import fetch_rss, fetch_ics, fetch_html, fetch_eventbrite, fetch_bandsintown, fetch_macaronikid_fxbg, fetch_freepress_calendar
from utils import hash_event, parse_when, categorize_text
from bs4 import BeautifulSoup


HTML_TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"[ \t\f\v]+")
DATE_PREFIX_RE = re.compile(r"^[A-Za-z]{3}\s+\d{1,2},\s+\d{4}:\s+")
TRAILING_AT_RE = re.compile(r"\s+at\s+(.+)$", re.IGNORECASE)

BOILERPLATE_LINE_PATTERNS = [
    re.compile(r"^\s*view on site\s*$", re.I),
    re.compile(r"^\s*\|\s*$"),
    re.compile(r"^\s*email this event\s*$", re.I),
]

def strip_html_to_text(html: str) -> str:
    """Turn HTML into plain text with REAL newlines (not literal \\n)."""
    if not html:
        return ""
    try:
        txt = BeautifulSoup(html, "html.parser").get_text("\n")
    except Exception:
        txt = HTML_TAG_RE.sub("", html)
        txt = txt.replace("&nbsp;", " ").replace("&amp;", "&")
    lines = [WS_RE.sub(" ", ln).strip() for ln in txt.splitlines()]
    lines = [ln for ln in lines if ln]
    # return real newline characters; ics.py will escape/fold correctly
    return "\n".join(lines)

def tidy_desc_text(text: str) -> str:
    """Remove boilerplate lines and collapse whitespace; keep meaningful lines."""
    if not text:
        return ""
    out = []
    for ln in text.splitlines():
        s = ln.strip()
        if not s:
            continue
        # drop boilerplate markers like "View on site", a lone "|" etc.
        if any(pat.match(s) for pat in BOILERPLATE_LINE_PATTERNS):
            continue
        out.append(s)
    # collapse runs of duplicate lines while preserving order
    dedup = []
    seen = set()
    for s in out:
        key = s.lower()
        if key in seen:
            continue
        seen.add(key)
        dedup.append(s)
    return "\n".join(dedup)

def add_html_description(event_obj, html: str):
    if not html:
        return
    try:
        if ContentLine:
            event_obj.extra.append(
                ContentLine(name="X-ALT-DESC", params={"FMTTYPE": "text/html"}, value=html)
            )
    except Exception:
        # best-effort; ignore if ics library doesn't support extra lines
        pass


DATA_EVENTS = 'data/events.json'
DOCS_DIR = 'docs'

def _clean_title_and_location(raw_title: str, existing_loc: str | None) -> tuple[str, str | None]:
    """Remove 'Mon dd, yyyy: ' prefix and peel trailing ' at Location' into LOCATION if not provided."""
    title = (raw_title or "").strip()
    title = DATE_PREFIX_RE.sub("", title).strip()
    loc = (existing_loc or "").strip()
    if not loc:
        m = TRAILING_AT_RE.search(title)
        if m:
            loc = m.group(1).strip()
            title = TRAILING_AT_RE.sub("", title).strip()
    return title, (loc or None)

def normalize_event(raw, timezone='America/New_York'):
    title = (raw.get('title') or '').strip()
    desc = (raw.get('description') or '').strip()
    loc  = (raw.get('location') or '').strip()
    link = raw.get('link')
    start = raw.get('start')
    end   = raw.get('end')
    local = tz.gettz(timezone)

    # clean up title and location (strip date prefix and trailing " at …")
    title, loc2 = _clean_title_and_location(title, loc)
    if loc2 is not None:
        loc = loc2

    def to_dt(x):
        if not x: return None
        try:
            dt = parser.parse(x)
            if not dt.tzinfo: dt = dt.replace(tzinfo=local)
            return dt
        except Exception:
            return None

    sdt = start if isinstance(start, datetime) else to_dt(start)
    edt = end if isinstance(end, datetime) else to_dt(end)
    if not sdt:
        sdt, edt2 = parse_when(desc or title, default_tz=timezone)
        if sdt and not edt:
            edt = edt2
    if not title or not sdt:
        return None
    if not edt:
        edt = sdt + timedelta(hours=2)

    return {
        'title': title,
        'description': desc,
        'location': loc,
        'start': sdt,
        'end': edt,
        'link': link,
        'source': raw.get('source'),
    }

def to_ics_event(ev):
    e = Event()
    # SUMMARY
    e.name = ev['title']

    # DTSTART/DTEND
    e.begin = ev['start']
    e.end = ev['end']

    # LOCATION
    if ev.get('location'):
        e.location = ev['location']

    # DESCRIPTION (plain text) + X-ALT-DESC (html if original looked like HTML)
    desc_html = ev.get('description') or ''
    if '<' in desc_html and '>' in desc_html:
        desc_text = strip_html_to_text(desc_html)
    else:
        desc_text = desc_html
    desc_text = tidy_desc_text(desc_text)

    # add link as a final line if not already present
    link = ev.get('link')
    if link and (link not in desc_text.split()):
        desc_text = (desc_text + ("\n" if desc_text else "") + link)

    if desc_text:
        e.description = desc_text

    # If it appears to be HTML, also include HTML alt description
    if desc_html and (('<' in desc_html and '>' in desc_html) or desc_html.strip().startswith('&lt;')):
        add_html_description(e, desc_html)

    # Stable UID from our own hash (set later once id is attached)
    if 'id' in ev:
        try:
            e.uid = ev['id']
        except Exception:
            pass

    # Metadata
    now_utc = datetime.now(timezone.utc)
    try:
        e.created = now_utc
        e.last_modified = now_utc
    except Exception:
        pass

    return e

def build_cals(events, out_dir):
    family = Calendar()
    adult = Calendar()
    recurring = Calendar()

    # Optional calendar metadata
    try:
        for cal in (family, adult, recurring):
            cal.scale = "GREGORIAN"
            cal.method = None
            cal.creator = "fxbg-event-feeds"
    except Exception:
        pass

    for ev in events:
        # ensure UID exists for stability
        if 'id' not in ev:
            ev['id'] = hash_event(ev['title'], ev['start'], ev.get('location',''))
        if ev['category'] == 'family':
            family.events.add(to_ics_event(ev))
        elif ev['category'] == 'recurring':
            recurring.events.add(to_ics_event(ev))
        else:
            adult.events.add(to_ics_event(ev))

    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, 'family.ics'), 'w', encoding='utf-8', newline='\n') as f:
        f.writelines(family.serialize_iter())
    with open(os.path.join(out_dir, 'adult.ics'), 'w', encoding='utf-8', newline='\n') as f:
        f.writelines(adult.serialize_iter())
    with open(os.path.join(out_dir, 'recurring.ics'), 'w', encoding='utf-8', newline='\n') as f:
        f.writelines(recurring.serialize_iter())

def main():
    cfg = yaml.safe_load(open('config.yaml','r',encoding='utf-8'))
    timezone = cfg.get('timezone', 'America/New_York')
    rules = cfg.get('keywords', {})
    keep_days = int(cfg.get('max_future_days', 365))

    collected = []
    debug = bool(os.getenv('FEEDS_DEBUG'))
    debug = bool(os.getenv('FEEDS_DEBUG'))

    for src in cfg.get('sources', []):
        if debug: print(f"→ Fetching: {src.get('name')} [{src.get('type')}] {src.get('url')}")
        if debug: print(f"→ Fetching: {src.get('name')} [{src.get('type')}] {src.get('url')}")
        t = src.get('type')
        try:
            if t == 'rss':
                got = fetch_rss(src['url']); collected += got; print(f"   rss events: {len(got)}") if debug else None
            elif t == 'ics':
                got = fetch_ics(src['url']); collected += got; print(f"   ics events: {len(got)}") if debug else None
            elif t == 'html':
                got = fetch_html(src['url'], src.get('html', {})); collected += got; print(f"   html events: {len(got)}") if debug else None
            elif t == 'eventbrite' and cfg.get('enable_eventbrite', True):
                token = os.getenv('EVENTBRITE_TOKEN') or cfg.get('eventbrite_token')
                got = fetch_eventbrite(src['url'], token_env=token); collected += got; print(f"   eventbrite events: {len(got)}") if debug else None
            elif t == 'bandsintown' and cfg.get('enable_bandsintown', True):
                appid = os.getenv('BANDSINTOWN_APP_ID') or cfg.get('bandsintown_app_id')
                got = fetch_bandsintown(src['url'], app_id_env=appid); collected += got; print(f"   bandsintown events: {len(got)}") if debug else None
            elif t == 'macaronikid_fxbg':
                got = fetch_macaronikid_fxbg(); collected += got; 
                print(f"   macaroni events: {len(got)}") if debug else None
            elif t == 'freepress':
                got = fetch_freepress_calendar(); collected += got; 
                print(f"   freepress events: {len(got)}") if debug else None
        except Exception as e:
            print("WARN source failed:", src.get('name'), e)

    for m in cfg.get('manual_events', []):
        collected.append({
            'title': m.get('title'),
            'description': m.get('description'),
            'location': m.get('location'),
            'start': m.get('start'),
            'end': m.get('end'),
            'source': 'manual',
            'link': m.get('link'),
        })

    norm = []
    for raw in collected:
        ev = normalize_event(raw, timezone=timezone)
        if not ev:
            if os.getenv('FEEDS_DEBUG'):
                ttl = (raw.get('title') or '')[:120]
                src = raw.get('source')
                dt = raw.get('start') or ''
                print(f"   · Dropped (no normalized datetime/title): '{ttl}' from {src} raw_start='{dt}'")
            continue

        # sanitize location if it looks like a time string, and round times
        def looks_like_time_or_range(txt: str) -> bool:
            if not txt: return False
            t = txt.lower()
            pat_range = r'(\d{1,2}(:\d{2})?\s*(a\.m\.|am|p\.m\.|pm))\s*[–\-to]{1,3}\s*(\d{1,2}(:\d{2})?\s*(a\.m\.|am|p\.m\.|pm))'
            pat_single = r'\b(\d{1,2}(:\d{2})?\s*(a\.m\.|am|p\.m\.|pm)|noon|midnight)\b'
            return bool(re.search(pat_range, t)) or bool(re.search(pat_single, t))

        if ev.get('location') and looks_like_time_or_range(ev['location']):
            ev['location'] = ''
        # round datetimes to the minute
        ev['start'] = ev['start'].replace(second=0, microsecond=0)
        if ev.get('end'):
            ev['end'] = ev['end'].replace(second=0, microsecond=0)

        ev['category'] = categorize_text(ev['title'], ev.get('description',''), rules)
        ev['id'] = hash_event(ev['title'], ev['start'], ev.get('location',''))
        norm.append(ev)

    dedup = {}
    for ev in norm:
        dedup[ev['id']] = ev

    now = datetime.now(tz=tz.gettz(timezone))
    horizon = now + timedelta(days=keep_days)
    filtered = [e for e in dedup.values() if e['end'] >= now - timedelta(days=2) and e['start'] <= horizon]
    filtered.sort(key=lambda x: x['start'])

    os.makedirs('data', exist_ok=True)
    with open('data/events.json', 'w', encoding='utf-8') as f:
        json.dump({'events': filtered}, f, indent=2, default=str)

    build_cals(filtered, DOCS_DIR)
    print(f"Built {DOCS_DIR}/family.ics, adult.ics, recurring.ics with {len(filtered)} events.")

if __name__ == '__main__':
    main()
