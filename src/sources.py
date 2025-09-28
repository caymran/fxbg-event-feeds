import requests, feedparser
from bs4 import BeautifulSoup
from dateutil import parser
from utils import parse_when

def fetch_rss(url):
    feed = feedparser.parse(url)
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

def fetch_ics(url):
    text = requests.get(url, timeout=30).text
    events = []
    chunks = text.split("BEGIN:VEVENT")
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

def text(el):
    return el.get_text(" ", strip=True) if el else None

def select_text(el, selector):
    if not selector: return None
    node = el.select_one(selector)
    return text(node) if node else None

def fetch_html(url, css):
    html = requests.get(url, timeout=30).text
    soup = BeautifulSoup(html, 'lxml')
    items = soup.select(css.get('item')) if css.get('item') else []
    out = []
    for el in items:
        title = select_text(el, css.get('title'))
        date_text = select_text(el, css.get('date')) or select_text(el, css.get('time'))
        loc = select_text(el, css.get('location'))
        desc = select_text(el, css.get('description'))
        start, end = parse_when(date_text)
        out.append({
            'title': title,
            'description': desc,
            'link': None,
            'start': start.isoformat() if start else None,
            'end': end.isoformat() if end else None,
            'location': loc,
            'source': url,
        })
    # fall back to page-level parse if nothing found
    if not out:
        t = soup.select_one('h1, h2, .title')
        d = soup.select_one('time, .date, p')
        start, end = parse_when(text(d))
        if t:
            out.append({
                'title': text(t),
                'description': text(soup.select_one('body'))[:500] if soup.select_one('body') else None,
                'link': url,
                'start': start.isoformat() if start else None,
                'end': end.isoformat() if end else None,
                'location': None,
                'source': url,
            })
    return [e for e in out if e.get('title')]

def fetch_facebook_page(page_id, token):
    if not token:
        return []
    fields = "name,place,start_time,end_time,description"
    url = f"https://graph.facebook.com/v18.0/{page_id}/events?time_filter=upcoming&fields={fields}&access_token={token}"
    try:
        r = requests.get(url, timeout=30)
        if r.status_code != 200:
            return []
        data = r.json()
        out = []
        for ev in data.get('data', []):
            start = ev.get('start_time')
            end = ev.get('end_time')
            place = (ev.get('place') or {}).get('name')
            out.append({
                'title': ev.get('name'),
                'description': ev.get('description'),
                'link': None,
                'start': start,
                'end': end,
                'location': place,
                'source': f"facebook:{page_id}",
            })
        return out
    except Exception:
        return []
