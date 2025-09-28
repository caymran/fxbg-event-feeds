import os, json, yaml
from datetime import datetime, timedelta
from dateutil import tz, parser
from ics import Calendar, Event
from sources import fetch_rss, fetch_ics, fetch_html, fetch_facebook_page
from utils import hash_event, parse_when, categorize_text

DATA_EVENTS = 'data/events.json'
DOCS_DIR = 'docs'

def load_json(path, default):
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return default

def save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, default=str)

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
        # attempt parse from desc/title
        sdt, edt = parse_when(desc or title, default_tz=timezone)

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
        desc = (desc + f"\\n{ev['link']}").strip()
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
    enable_fb = cfg.get('enable_facebook', False)
    fb_token = os.getenv('FACEBOOK_TOKEN')

    collected = []

    for src in cfg.get('sources', []):
        t = src.get('type')
        try:
            if t == 'rss':
                collected += fetch_rss(src['url'])
            elif t == 'ics':
                collected += fetch_ics(src['url'])
            elif t == 'html':
                collected += fetch_html(src['url'], src.get('html', {}))
            elif t == 'facebook_page' and enable_fb:
                collected += fetch_facebook_page(src['page_id'], fb_token)
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
        ev['category'] = categorize_text(ev['title'], ev.get('description',''), rules)
        ev['id'] = hash_event(ev['title'], ev['start'], ev.get('location',''))
        norm.append(ev)

    # dedupe: latest wins
    dedup = {}
    for ev in norm:
        dedup[ev['id']] = ev

    # keep if ends >= 2 days ago
    now = datetime.now(tz=tz.gettz(timezone))
    filtered = [e for e in dedup.values() if e['end'] >= now - timedelta(days=2)]
    filtered.sort(key=lambda x: x['start'])

    # persist
    with open('data/events.json', 'w', encoding='utf-8') as f:
        json.dump({'events': filtered}, f, indent=2, default=str)

    build_cals(filtered, DOCS_DIR)
    print(f"Built {DOCS_DIR}/family.ics, adult.ics, recurring.ics with {len(filtered)} events.")

if __name__ == '__main__':
    main()
