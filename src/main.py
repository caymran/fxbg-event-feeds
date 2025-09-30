# src/main.py
import os
import json
import yaml
import fnmatch
import re
import logging
from datetime import datetime, timedelta, timezone
from dateutil import tz, parser
from ics import Calendar, Event
from bs4 import BeautifulSoup
from urllib.parse import urlsplit

# ---- Add TRACE level ---------------------------------------------------------
TRACE = 5
logging.addLevelName(TRACE, "TRACE")

def _trace(self, msg, *args, **kwargs):
    if self.isEnabledFor(TRACE):
        self._log(TRACE, msg, args, **kwargs)

logging.Logger.trace = _trace  # type: ignore[attr-defined]

def _init_logging():
    # FEEDS_LOG_LEVEL overrides; otherwise honor FEEDS_TRACE/FEEDS_DEBUG
    env_level = os.getenv("FEEDS_LOG_LEVEL", "").upper().strip()
    if not env_level:
        if os.getenv("FEEDS_TRACE"):
            env_level = "TRACE"
        elif os.getenv("FEEDS_DEBUG"):
            env_level = "DEBUG"
        else:
            env_level = "INFO"

    level_map = {
        "CRITICAL": logging.CRITICAL,
        "ERROR": logging.ERROR,
        "WARNING": logging.WARNING,
        "INFO": logging.INFO,
        "DEBUG": logging.DEBUG,
        "TRACE": TRACE,
        "NOTSET": logging.NOTSET,
    }
    level = level_map.get(env_level, logging.INFO)

    fmt = "%(asctime)s %(levelname)-5s %(name)s :: %(message)s"
    logging.basicConfig(level=level, format=fmt)

_init_logging()
log = logging.getLogger("main")

# ---- ics ContentLine compatibility shim -------------------------------------
try:
    from ics.grammar.parse import ContentLine
except Exception:
    try:
        from ics.grammar.line import ContentLine  # type: ignore
    except Exception:
        ContentLine = None  # type: ignore

# ---- Source import surface ---------------------------------------------------
from sources import (
    fetch_rss,
    fetch_ics,
    fetch_html,
    fetch_eventbrite,
    fetch_bandsintown,
    fetch_freepress_calendar,
    fetch_thrillshare_ical,
    fetch_macaronikid_fxbg,                 # requests/cloudscraper/sitemap
    fetch_macaronikid_fxbg_playwright,      # may be None if playwright missing
)

# ---- Utils -------------------------------------------------------------------
from utils import hash_event, parse_when, categorize_text

HTML_TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"[ \t\f\v]+")
DATE_PREFIX_RE = re.compile(r"^[A-Za-z]{3}\s+\d{1,2},\s+\d{4}:\s+")
TRAILING_AT_RE = re.compile(r"\s+at\s+(.+)$", re.IGNORECASE)

BOILERPLATE_LINE_PATTERNS = [
    re.compile(r"^\s*view on site\s*$", re.I),
    re.compile(r"^\s*\|\s*$"),
    re.compile(r"^\s*email this event\s*$", re.I),
    re.compile(r"^\s*google map\s*$", re.I),
    re.compile(r"^\s*\+?\s*add to google calendar.*$", re.I),
    re.compile(r"^\s*\+?\s*add to apple calendar.*$", re.I),
    re.compile(r"^\s*get your free ticket here\s*$", re.I),
]

# Add to BOILERPLATE_LINE_PATTERNS
BOILERPLATE_LINE_PATTERNS += [
    re.compile(r"^\s*Eventbrite\b.*$", re.I),
    re.compile(r"^\s*Find my tickets\b.*$", re.I),
    re.compile(r"^\s*Log In\s*Sign Up\s*$", re.I),
    re.compile(r"^\s*Create Events\b.*$", re.I),
    re.compile(r"^\s*Solutions\b.*$", re.I),
    re.compile(r"^\s*Community Guidelines\b.*$", re.I),
    re.compile(r"^\s*Help Center\b.*$", re.I),
    re.compile(r"^\s*Privacy\b.*$", re.I),
    re.compile(r"^\s*Do Not Sell or Share My Personal Information\b.*$", re.I),
]

DATA_EVENTS = "data/events.json"
DOCS_DIR = "docs"

def _host_from(ev: dict) -> str:
    src = (ev.get("source") or "").strip()
    link = (ev.get("link") or "").strip()
    try:
        return (urlsplit(link or src).netloc or "").lower()
    except Exception:
        return ""

def strip_html_to_text(html: str) -> str:
    if not html:
        return ""
    try:
        txt = BeautifulSoup(html, "html.parser").get_text("\n")
    except Exception:
        txt = HTML_TAG_RE.sub("", html)
        txt = txt.replace("&nbsp;", " ").replace("&amp;", "&")
    lines = [WS_RE.sub(" ", ln).strip() for ln in txt.splitlines()]
    lines = [ln for ln in lines if ln]
    return "\n".join(lines)

def tidy_desc_text(text: str) -> str:
    if not text:
        return ""
    out = []
    for ln in text.splitlines():
        s = ln.strip()
        if not s:
            continue
        if any(pat.match(s) for pat in BOILERPLATE_LINE_PATTERNS):
            continue
        out.append(s)
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
    if not html or not ContentLine:
        return
    try:
        event_obj.extra.append(
            ContentLine(name="X-ALT-DESC", params={"FMTTYPE": "text/html"}, value=html)
        )
    except Exception:
        pass

EVENTBRITE_LOC_START_RE = re.compile(r"\bLocation\b[:\s]*", re.I)
EVENTBRITE_LOC_STOP_MARKERS = [
    r"\bGet directions\b",
    r"\bGood to know\b",
    r"\bHighlights\b",
    r"\bAbout this event\b",
    r"\bTags\b",
    r"\bOrganized by\b",
    r"\bReport this event\b",
    r"\bFree\b",
    r"\bMultiple dates\b",
]

def _extract_eventbrite_location(big: str) -> str:
    """
    From a huge Eventbrite page-dump string that includes site chrome, pull out the
    address block that follows 'Location' and ends before common section markers.
    Returns '' if no confident extraction.
    """
    if not big:
        return ""
    # Normalize whitespace to simplify slicing.
    txt = WS_RE.sub(" ", strip_html_to_text(big)).strip()
    # Only try if it clearly looks like Eventbrite chrome
    if "Eventbrite" not in txt or "Find my tickets" not in txt:
        return ""

    # Find where 'Location' starts
    m = EVENTBRITE_LOC_START_RE.search(txt)
    if not m:
        return ""
    start_idx = m.end()

    # Find the earliest stop marker after start
    stop_idx = len(txt)
    for pat in EVENTBRITE_LOC_STOP_MARKERS:
        mm = re.search(pat, txt[start_idx:], flags=re.I)
        if mm:
            stop_idx = min(stop_idx, start_idx + mm.start())

    chunk = txt[start_idx:stop_idx].strip(" -–—|")
    # De-duplicate repeated address lines like "320 Emancipation Hwy 320 Emancipation Highway ..."
    # Heuristic: collapse triple+ spaces, remove consecutive duplicate tokens.
    parts = [p.strip() for p in re.split(r"[,\s]{2,}", chunk) if p.strip()]
    dedup = []
    seen = set()
    for p in parts:
        key = p.lower()
        if key in seen:
            continue
        seen.add(key)
        dedup.append(p)
    # Rebuild; prefer commas between likely address tokens
    loc = ", ".join(dedup)
    # Trim obvious trailing noise like ZIP repeated twice, or dangling words.
    loc = re.sub(r"(?:,?\s*(Get directions|Good to know|Highlights).*)$", "", loc, flags=re.I).strip(", ")
    return loc

# ---- modify existing clean_location_field ----
def clean_location_field(raw_loc: str) -> str:
    if not raw_loc:
        return ""
    s = raw_loc.strip()
    if "<" in s and ">" in s:
        try:
            from bs4 import BeautifulSoup as _BS
            s = _BS(s, "html.parser").get_text(" ")
        except Exception:
            s = HTML_TAG_RE.sub("", s)
    s = WS_RE.sub(" ", s).strip(" -–—|")

    # NEW: If it looks like the giant Eventbrite page dump, extract the true location.
    if ("Eventbrite" in s and "Find my tickets" in s) or len(s) > 400:
        eb_loc = _extract_eventbrite_location(s)
        if eb_loc:
            return eb_loc

    return s


def _clean_title_and_location(raw_title: str, existing_loc: str | None) -> tuple[str, str | None]:
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

    title, loc2 = _clean_title_and_location(title, loc)
    if loc2 is not None:
        loc = loc2
    loc = clean_location_field(loc)

    # If location is still empty or still looks like EB chrome, try to extract from description
    if (not loc) or ("Eventbrite" in loc and "Find my tickets" in loc):
        maybe = _extract_eventbrite_location(desc or "")
        if maybe:
            loc = maybe

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
        log.debug("normalize_event: drop (missing title/start) title=%r start=%r src=%r", title, start, raw.get("source"))
        return None
    if not edt:
        edt = sdt + timedelta(hours=2)

    out = {
        'title': title,
        'description': desc,
        'location': loc,
        'start': sdt,
        'end': edt,
        'link': link,
        'source': raw.get('source'),
    }
    log.trace("normalize_event -> %s @ %s", out['title'], out['start'])
    return out

def to_ics_event(ev):
    e = Event()
    e.name = ev['title']
    e.begin = ev['start']
    e.end = ev['end']

    if ev.get('location'):
        e.location = ev['location']

    desc_html = ev.get('description') or ''
    if '<' in desc_html and '>' in desc_html:
        desc_text = strip_html_to_text(desc_html)
    else:
        desc_text = desc_html
    desc_text = tidy_desc_text(desc_text)

    link = ev.get('link')
    if link and (link not in desc_text.split()):
        desc_text = (desc_text + ("\n" if desc_text else "") + link)

    if desc_text:
        e.description = desc_text

    if desc_html and (('<' in desc_html and '>' in desc_html) or desc_html.strip().startswith('&lt;')):
        add_html_description(e, desc_html)

    if 'id' in ev:
        try:
            e.uid = ev['id']
        except Exception:
            pass

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
    sports = Calendar()

    cat_counts = {"family": 0, "adult": 0, "recurring": 0, "sports": 0}

    for ev in events:
        if ev['category'] == 'family':
            family.events.add(to_ics_event(ev)); cat_counts["family"] += 1
        elif ev['category'] == 'recurring':
            recurring.events.add(to_ics_event(ev)); cat_counts["recurring"] += 1
        elif ev['category'] == 'sports':
            sports.events.add(to_ics_event(ev)); cat_counts["sports"] += 1
        else:
            adult.events.add(to_ics_event(ev)); cat_counts["adult"] += 1

    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, 'family.ics'), 'w', encoding='utf-8', newline='\n') as f:
        f.writelines(family.serialize_iter())
    with open(os.path.join(out_dir, 'adult.ics'), 'w', encoding='utf-8', newline='\n') as f:
        f.writelines(adult.serialize_iter())
    with open(os.path.join(out_dir, 'recurring.ics'), 'w', encoding='utf-8', newline='\n') as f:
        f.writelines(recurring.serialize_iter())
    with open(os.path.join(out_dir, 'sports.ics'), 'w', encoding='utf-8', newline='\n') as f:
        f.writelines(sports.serialize_iter())

    log.info("Wrote calendars to %s (family=%d, adult=%d, recurring=%d, sports=%d)",
             out_dir, cat_counts["family"], cat_counts["adult"], cat_counts["recurring"], cat_counts["sports"])

def _looks_like_time_or_range(txt: str) -> bool:
    if not txt: return False
    t = txt.lower()
    pat_range = r'(\d{1,2}(:\d{2})?\s*(a\.m\.|am|p\.m\.|pm))\s*[–\-to]{1,3}\s*(\d{1,2}(:\d{2})?\s*(a\.m\.|am|p\.m\.|pm))'
    pat_single = r'\b(\d{1,2}(:\d{2})?\s*(a\.m\.|am|p\.m\.|pm)|noon|midnight)\b'
    return bool(re.search(pat_range, t)) or bool(re.search(pat_single, t))

def route_to_sports(ev: dict, cfg: dict) -> bool:
    rt = cfg.get("route_to_sports", {})
    title = (ev.get("title") or "").strip()
    location = (ev.get("location") or "").strip()
    host = _host_from(ev)

    for dom in rt.get("domains", []):
        dom = dom.lower().strip()
        if dom and host.endswith(dom):
            return True

    for pat in rt.get("title_regex", []):
        try:
            if re.search(pat, title, re.IGNORECASE):
                return True
        except re.error:
            pass

    for pat in rt.get("title_glob", []):
        if fnmatch.fnmatch(title.lower(), pat.lower()):
            return True

    for pat in rt.get("location_regex", []):
        try:
            if re.search(pat, location, re.IGNORECASE):
                return True
        except re.error:
            pass

    return False

def is_dropped(ev: dict, cfg: dict) -> bool:
    drops = cfg.get("drop", {})
    title = (ev.get("title") or "").strip()
    location = (ev.get("location") or "").strip()
    source = (ev.get("source") or "").strip()
    link = (ev.get("link") or "").strip()
    host = ""
    try:
        host = urlsplit(link or source).netloc.lower()
    except Exception:
        pass

    for dom in drops.get("domains", []):
        dom = dom.lower().strip()
        if dom and host.endswith(dom):
            return True

    for pat in drops.get("title_regex", []):
        try:
            if re.search(pat, title, re.IGNORECASE):
                return True
        except re.error:
            pass

    for pat in drops.get("title_glob", []):
        if fnmatch.fnmatch(title.lower(), pat.lower()):
            return True

    for pat in drops.get("location_regex", []):
        try:
            if re.search(pat, location, re.IGNORECASE):
                return True
        except re.error:
            pass

    return False

def main():
    cfg = yaml.safe_load(open('config.yaml','r',encoding='utf-8'))
    timezone = cfg.get('timezone', 'America/New_York')
    rules = cfg.get('keywords', {})
    keep_days = int(cfg.get('max_future_days', 365))

    collected = []

    log.info("Config: tz=%s keep_days=%s sources=%d", timezone, keep_days, len(cfg.get('sources', [])))
    if os.getenv("FEEDS_DEBUG") or os.getenv("FEEDS_TRACE"):
        log.debug("Keywords buckets: %s", list(rules.keys()))

    for src in cfg.get('sources', []):
        name = src.get('name')
        typ = src.get('type')
        url = src.get('url')
        log.info("→ Fetching: %s [%s] %s", name, typ, url)

        try:
            got = []
            if typ == 'rss':
                got = fetch_rss(url); log.debug("   rss events: %d", len(got))
            elif typ == 'ics':
                got = fetch_ics(url); log.debug("   ics events: %d", len(got))
            elif typ == 'thrillshare_ical':
                got = fetch_thrillshare_ical(url); log.debug("   thrillshare ICS events: %d", len(got))
            elif typ == 'html':
                got = fetch_html(url, src.get('html', {})); log.debug("   html events: %d", len(got))
            elif typ == 'eventbrite' and cfg.get('enable_eventbrite', True):
                token = os.getenv('EVENTBRITE_TOKEN') or cfg.get('eventbrite_token')
                got = fetch_eventbrite(url, token_env=token); log.debug("   eventbrite events: %d", len(got))
            elif typ == 'bandsintown' and cfg.get('enable_bandsintown', True):
                appid = os.getenv('BANDSINTOWN_APP_ID') or cfg.get('bandsintown_app_id')
                got = fetch_bandsintown(url, app_id_env=appid); log.debug("   bandsintown events: %d", len(got))
            elif typ == 'macaronikid_fxbg':
                if fetch_macaronikid_fxbg_playwright:
                    log.debug("   MacKID: trying Playwright crawler …")
                    try:
                        # headless can be toggled via FEEDS_PW_HEADLESS=0
                        headless = os.getenv("FEEDS_PW_HEADLESS", "1") != "0"
                        got = fetch_macaronikid_fxbg_playwright(headless=headless)
                    except Exception as e:
                        log.warning("   MacKID (PW) failed, falling back to requests: %s", e)
                        got = []
                if not got:
                    log.debug("   MacKID: using requests/sitemap fallback …")
                    got = fetch_macaronikid_fxbg()
                log.info("   macaroni events: %d", len(got))
            elif typ == 'freepress':
                got = fetch_freepress_calendar(url)
                log.info("   freepress events: %d", len(got))
            else:
                log.warning("Unknown source type %r for %s", typ, name)
                got = []
            collected += got
        except Exception as e:
            log.exception("WARN source failed: %s (%s)", name, e)

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

    log.info("Collected raw events: %d", len(collected))

    norm = []
    for raw in collected:
        ev = normalize_event(raw, timezone=timezone)
        if not ev:
            ttl = (raw.get('title') or '')[:120]
            src = raw.get('source')
            dtv = raw.get('start') or ''
            log.debug("   · Dropped (no normalized datetime/title): '%s' from %s raw_start='%s'", ttl, src, dtv)
            continue

        if ev.get('location') and _looks_like_time_or_range(ev['location']):
            log.trace("location looked like time; clearing: %r", ev['location'])
            ev['location'] = ''

        ev['start'] = ev['start'].replace(second=0, microsecond=0)
        if ev.get('end'):
            ev['end'] = ev['end'].replace(second=0, microsecond=0)

        ev['category'] = categorize_text(ev['title'], ev.get('description',''), rules)

        host = _host_from(ev)
        if (ev.get('source') in ('macaronikid', 'thrillshare')
            or host.endswith('fxbgschools.us')
            or 'macaronikid.com' in host):
            ev['category'] = 'family'

        ev['id'] = hash_event(ev['title'], ev['start'], ev.get('location',''))

        if route_to_sports(ev, cfg):
            ev['category'] = 'sports'

        if is_dropped(ev, cfg):
            log.debug("   · Dropped by rule: '%s' (%s)", ev['title'], ev.get('source'))
            continue

        norm.append(ev)

    dedup = {}
    for ev in norm:
        dedup[ev['id']] = ev

    now = datetime.now(tz=tz.gettz(timezone))
    horizon = now + timedelta(days=keep_days)
    filtered = [e for e in dedup.values() if e['end'] >= now - timedelta(days=2) and e['start'] <= horizon]
    filtered.sort(key=lambda x: x['start'])

    os.makedirs('data', exist_ok=True)
    with open(DATA_EVENTS, 'w', encoding='utf-8') as f:
        json.dump({'events': filtered}, f, indent=2, default=str)
    log.info("Wrote %s (events=%d)", DATA_EVENTS, len(filtered))

    build_cals(filtered, DOCS_DIR)
    log.info("Built %s/family.ics, adult.ics, recurring.ics, sports.ics with %d events total.", DOCS_DIR, len(filtered))

if __name__ == '__main__':
    main()
