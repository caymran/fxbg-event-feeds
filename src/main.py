import os, json, yaml
from datetime import datetime, timedelta
from dateutil import tz, parser
from ics import Calendar, Event
from sources import fetch_rss, fetch_ics, fetch_html, fetch_eventbrite, fetch_bandsintown
from utils import hash_event, parse_when, categorize_text

DATA_EVENTS = 'data/events.json'
DOCS_DIR = 'docs'

def normalize_event(raw, timezone='America/New_York'):
    title = (raw.get('title') or '').strip()
    desc = (raw.get('description') or '').strip()
    loc  = (raw.get('location') or '').strip()
    link = raw.get('link')
    start = raw.get('start')
    end   = raw.get('end')
    local = tz.gettz(timezone)

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
        'source': raw.get('source')
    }

def to_ics_event(ev):
    e = Event()
    e.name = ev['title']
    e.begin = ev['start']
    e.end = ev['end']
    if ev.get('location'):
        e.location = ev['location']
    desc = ev.get('description') or ''
    if ev.get('link'):
        desc = (desc + f"\n{ev['link']}").strip()
    if desc:
        e.description = desc
    return e

def build_cals(events, out_dir):
    family = Calendar()
    adult = Calendar()
    recurring = Calendar()

    for ev in events:
        if ev['category'] == 'family':
            family.events.add(to_ics_event(ev))
        elif ev['category'] == 'recurring':
            recurring.events.add(to_ics_event(ev))
        else:
            adult.events.add(to_ics_event(ev))

    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, 'family.ics'), 'w', encoding='utf-8') as f:
        f.writelines(family.serialize_iter())
    with open(os.path.join(out_dir, 'adult.ics'), 'w', encoding='utf-8') as f:
        f.writelines(adult.serialize_iter())
    with open(os.path.join(out_dir, 'recurring.ics'), 'w', encoding='utf-8') as f:
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
            continue
        # sanitize location if it looks like a time string, and round times
        import re
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
