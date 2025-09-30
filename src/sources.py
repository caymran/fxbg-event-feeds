import os
import time
import json
import base64
import logging
import re
import hashlib
import requests
import feedparser
import urllib.parse
from bs4 import BeautifulSoup
from dateutil import parser
from urllib.robotparser import RobotFileParser

from utils import parse_when, jitter_sleep

CACHE_PATH = "data/cache.json"

LOG = logging.getLogger("sources")
HTTP_LOG = logging.getLogger("sources.http")


def load_cache():
    if os.path.exists(CACHE_PATH):
        try:
            return json.load(open(CACHE_PATH, "r", encoding="utf-8"))
        except Exception:
            return {"http_cache": {}}
    return {"http_cache": {}}


def save_cache(cache):
    os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
    json.dump(cache, open(CACHE_PATH, "w", encoding="utf-8"), indent=2)


def robots_allowed(url, user_agent="*"):
    """
    Basic robots.txt guard with allowlist shortcuts for known-safe endpoints.
    Explicitly allow data: URLs and MacaroniKID per-event ICS.
    """
    # Always allow data: URLs
    if url.startswith("data:"):
        LOG.debug("robots_allowed: allow data: URL")
        return True

    ALLOWLIST_SUBSTR = [
        "/common/modules/iCalendar/iCalendar.aspx",
        "/calendar/1.xml",
        "/events/?ical=1",
        "/events/feed",
    ]
    try:
        host = urllib.parse.urlsplit(url).netloc.lower()
        # Macaroni KID per-event ICS
        if host.endswith("macaronikid.com") and url.lower().endswith(".ics"):
            return True
        # Eventbrite discovery + event pages
        parts = urllib.parse.urlsplit(url)
        if parts.netloc.endswith("eventbrite.com") and (
            "/d/" in parts.path or "/e/" in parts.path
        ):
            return True
    except Exception as ex:
        LOG.debug("robots_allowed: urlsplit error (%s) → allow", str(ex))
        return True

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
        if os.getenv("FEEDS_DEBUG") and not allowed:
            LOG.debug("robots.txt disallows: %s", url)
        return allowed
    except Exception as ex:
        LOG.debug("robots_allowed: fallback allow %s (%s)", url, str(ex))
        return True


def _cache_key(url, headers):
    # include Authorization + User-Agent so cached bodies don't leak across creds
    h = headers or {}
    auth = h.get("Authorization", "")
    ua = h.get("User-Agent", "")
    return url + "||" + hashlib.sha1((auth + "|" + ua).encode("utf-8")).hexdigest()


def req_with_cache(url, headers=None, throttle=(2, 5), max_retries=3):
    """
    Cached GET with ETag/If-Modified-Since support, retry/backoff, and a special
    fast-path for data: URLs (e.g., MacKID per-event ICS buttons).
    """
    # --- Special case: data: URLs ---
    if url.startswith("data:"):
        try:
            meta, data_part = url.split(",", 1)
        except ValueError:
            HTTP_LOG.warning("HTTP data: malformed (no comma): %s", url[:140])
            return 400, "", {}
        # RFC2397 data:[<mediatype>][;base64],<data>
        is_base64 = ";base64" in meta.lower()
        try:
            raw_bytes = urllib.parse.unquote_to_bytes(data_part)
            body_bytes = base64.b64decode(raw_bytes) if is_base64 else raw_bytes
            body = body_bytes.decode("utf-8", errors="replace")
            HTTP_LOG.debug("HTTP(GET data:) %s -> 200 (len=%d)", meta[:140], len(body))
            return 200, body, {}
        except Exception as ex:
            HTTP_LOG.warning("HTTP data: decode error: %s", str(ex))
            return 400, "", {}

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
            HTTP_LOG.debug(
                "HTTP GET %s | headers: UA=%r auth=%s etag=%s ims=%s",
                url,
                headers.get("User-Agent"),
                "yes" if "Authorization" in headers else "no",
                entry.get("etag"),
                entry.get("last_modified"),
            )
            resp = session.get(url, headers=headers, timeout=30)
            if resp.status_code == 304:
                body = entry.get("body", "")
                HTTP_LOG.debug("HTTP %s -> 304 (using cache len=%d)", url, len(body))
                return 304, body, {}
            if resp.status_code in (200, 201):
                etag = resp.headers.get("ETag")
                lastmod = resp.headers.get("Last-Modified")
                body = resp.text
                cache["http_cache"][key] = {
                    "etag": etag,
                    "last_modified": lastmod,
                    "fetched_at": int(time.time()),
                    "body": body[:500000],
                }
                save_cache(cache)
                HTTP_LOG.debug(
                    "HTTP %s -> %d in cache (len=%d)", url, resp.status_code, len(body)
                )
                jitter_sleep(throttle[0], throttle[1])
                return resp.status_code, body, {"etag": etag, "last_modified": lastmod}
            if resp.status_code in (429, 500, 502, 503, 504):
                HTTP_LOG.warning(
                    "HTTP %s -> %d (retry in %ss)", url, resp.status_code, backoff
                )
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            HTTP_LOG.debug("HTTP %s -> %d (no body cached)", url, resp.status_code)
            return resp.status_code, "", {}
        except requests.RequestException as ex:
            HTTP_LOG.warning("HTTP %s error: %s (retry in %ss)", url, str(ex), backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
    HTTP_LOG.error("HTTP %s failed after %d attempts", url, max_retries)
    return 599, "", {}


def fetch_thrillshare_ical(events_page_url, user_agent="fxbg-event-bot/1.0"):
    """
    Load a Thrillshare events page (e.g., https://gwes.fxbgschools.us/o/gwes/events),
    find the 'Click to Download Calendar' link to the generate_ical endpoint,
    then fetch & parse that ICS for all events.
    """
    if not robots_allowed(events_page_url, user_agent):
        return []
    status, body, _ = req_with_cache(
        events_page_url, headers={"User-Agent": user_agent}, throttle=(1, 3)
    )
    if status != 200 or not body:
        return []

    soup = BeautifulSoup(body, "html.parser")
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


def _parse_eventbrite_detail(detail_url, user_agent=None, default_tz="America/New_York"):
    """
    Parse a single Eventbrite event page; prefer JSON-LD @type=Event, with
    solid fallbacks for title/date/location from visible HTML/meta.
    """
    from dateutil import parser as dtp, tz as dttz

    user_agent = user_agent or os.getenv("EB_UA") or (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    )
    headers = {"User-Agent": user_agent, "Accept-Language": "en-US,en;q=0.9"}

    st, body, _ = req_with_cache(detail_url, headers=headers, throttle=(1, 3))
    if os.getenv("FEEDS_DEBUG"):
        LOG.debug("     · EB detail GET %s: %s", st, detail_url)
    if st != 200 or not body:
        return None

    soup = BeautifulSoup(body, "html.parser")

    def _to_iso(val):
        if not val:
            return None
        try:
            dt = dtp.parse(val)
            if not dt.tzinfo:
                dt = dt.replace(tzinfo=dttz.gettz(default_tz))
            return dt.isoformat()
        except Exception:
            return None

    ev_name = desc = start = end = None
    location_str = ""

    # ---- 1) JSON-LD @type=Event (often the cleanest) ----
    for tag in soup.select('script[type="application/ld+json"]'):
        try:
            data = json.loads(tag.string or "")
        except Exception:
            continue

        def handle_evt(evt):
            nonlocal ev_name, desc, start, end, location_str
            nm = _eb_clean_text(evt.get("name") or "")
            sdt = evt.get("startDate") or evt.get("start_date")
            edt = evt.get("endDate") or evt.get("end_date")
            dsc = _eb_clean_text(evt.get("description") or "")
            loc = _eb_location_str(evt.get("location"))
            if nm and not ev_name:
                ev_name = nm
            if sdt and not start:
                start = _to_iso(sdt) or sdt
            if edt and not end:
                end = _to_iso(edt) or edt
            if dsc and not desc:
                desc = dsc
            if loc and not location_str:
                location_str = loc

        if isinstance(data, dict):
            if data.get("@type") in ("Event", "Festival"):
                handle_evt(data)
            for node in (data.get("@graph") or []):
                if isinstance(node, dict) and node.get("@type") in ("Event", "Festival"):
                    handle_evt(node)
        elif isinstance(data, list):
            for node in data:
                if isinstance(node, dict) and node.get("@type") in ("Event", "Festival"):
                    handle_evt(node)

    # ---- 2) Fallbacks from visible HTML ----
    if not ev_name:
        h = soup.select_one("h1, [data-testid='event-title'], [data-automation='listing-title']")
        if h:
            ev_name = _eb_clean_text(h.get_text(" ", strip=True))

    # Try meta/itemprop times
    if not (start or end):
        m_start = soup.select_one(
            "meta[itemprop='startDate'], meta[itemprop='startdate'], meta[property='event:start_time']"
        )
        m_end = soup.select_one(
            "meta[itemprop='endDate'], meta[itemprop='enddate'], meta[property='event:end_time']"
        )
        if m_start and m_start.get("content"):
            start = _to_iso(m_start["content"]) or start
        if m_end and m_end.get("content"):
            end = _to_iso(m_end["content"]) or end

    # <time datetime="...">
    if not (start or end):
        ts = [t.get("datetime") for t in soup.select("time[datetime]") if t.get("datetime")]
        if ts:
            start = start or _to_iso(ts[0]) or ts[0]
            if len(ts) > 1:
                end = end or _to_iso(ts[1]) or ts[1]

    # 'Date and time' block text -> parse_when
    if not (start or end):
        dt_blk = soup.find(
            lambda t: t.name in ("section", "div")
            and "Date and time" in t.get_text(" ", strip=True)
        )
        if dt_blk:
            sdt, edt = parse_when(_eb_clean_text(dt_blk.get_text(" ", strip=True)), default_tz=default_tz)
            if sdt:
                start = sdt.isoformat()
            if edt:
                end = edt.isoformat()

    # Location from visible blocks if JSON-LD didn’t give it
    if not location_str:
        loc_cont = soup.select_one(
            "[data-spec='event-details-location'], [data-testid='event-details-location']"
        )
        if loc_cont:
            txt = _eb_clean_text(loc_cont.get_text(" ", strip=True))
            if txt:
                location_str = txt
        if not location_str:
            loc_blk = soup.find(
                lambda t: t.name in ("section", "div")
                and re.search(r"\bLocation\b", t.get_text(" ", strip=True), re.I)
            )
            if loc_blk:
                lines = [
                    ln.strip()
                    for ln in loc_blk.get_text("\n", strip=True).splitlines()
                    if ln.strip()
                ]
                if lines:
                    if len(lines) >= 2:
                        location_str = _eb_clean_text(f"{lines[0]} - {' '.join(lines[1:])}")
                    else:
                        location_str = _eb_clean_text(lines[0])

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
    if os.getenv("FEEDS_DEBUG"):
        t = (ev_name or "")[:60]
        loc_snip = (location_str or "")[:60]
        LOG.debug("     · EB parsed: %s | start:%s end:%s loc:%s", t, start, end, loc_snip)
    return evt


def fetch_eventbrite_discovery_playwright(list_url, pages=10, user_agent=None):
    from playwright.sync_api import sync_playwright

    user_agent = user_agent or (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    )

    def _with_page(u, n):
        parts = list(urllib.parse.urlsplit(u))
        q = urllib.parse.parse_qs(parts[3])
        q["page"] = [str(n)]
        parts[3] = urllib.parse.urlencode({k: v[0] for k, v in q.items()})
        return urllib.parse.urlunsplit(parts)

    out, detail_urls = [], set()

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent=user_agent,
            locale="en-US",
            timezone_id="America/New_York",
            viewport={"width": 1366, "height": 900},
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
        )
        page = ctx.new_page()

        # 1) collect detail links
        for i in range(1, int(pages) + 1):
            u = _with_page(list_url, i)
            try:
                page.goto(u, wait_until="networkidle", timeout=45000)
                page.wait_for_timeout(400)
                hrefs = page.eval_on_selector_all(
                    "a[href*='/e/']", "els => els.map(e => e.getAttribute('href'))"
                ) or []
                for h in hrefs:
                    if not h:
                        continue
                    absu = urllib.parse.urljoin(u, h.split("?", 1)[0])
                    path = urllib.parse.urlsplit(absu).path
                    if path.startswith("/e/") and not any(
                        seg in path for seg in ("/organizer/", "/o/", "/collections/")
                    ):
                        detail_urls.add(absu)
                if os.getenv("FEEDS_DEBUG"):
                    LOG.debug("   EB(PW) page %d: links=%d cum", i, len(detail_urls))
            except Exception as ex:
                if os.getenv("FEEDS_DEBUG"):
                    LOG.debug("   EB(PW) page %d error: %s", i, str(ex)[:120])

        # 2) visit each detail and parse JSON-LD
        def parse_jsonld(soup):
            import json as _json

            title = desc = start = end = loc = None
            for tag in soup.select('script[type="application/ld+json"]'):
                try:
                    data = _json.loads(tag.string or "")
                except Exception:
                    continue

                def use(ev):
                    nonlocal title, desc, start, end, loc
                    if not isinstance(ev, dict):
                        return
                    if ev.get("@type") != "Event":
                        return
                    title = title or (ev.get("name") or "").strip()
                    desc = desc or (ev.get("description") or "")
                    start = start or (ev.get("startDate") or ev.get("start_date"))
                    end = end or (ev.get("endDate") or ev.get("end_date"))
                    locobj = ev.get("location")
                    if isinstance(locobj, dict):
                        nm = (locobj.get("name") or "").strip()
                        addr = locobj.get("address")
                        addr_txt = ""
                        if isinstance(addr, dict):
                            parts = [
                                addr.get("streetAddress"),
                                addr.get("addressLocality"),
                                addr.get("addressRegion"),
                                addr.get("postalCode"),
                            ]
                            addr_txt = " ".join(p for p in parts if p)
                        elif isinstance(addr, str):
                            addr_txt = addr.strip()
                        loc = loc or (f"{nm} - {addr_txt}".strip(" -") if (nm or addr_txt) else None)

                if isinstance(data, dict):
                    if data.get("@type") == "Event":
                        use(data)
                    for node in (data.get("@graph") or []):
                        use(node)
                elif isinstance(data, list):
                    for node in data:
                        use(node)
            return title, desc, start, end, loc

        for ev_url in sorted(detail_urls):
            try:
                page.goto(ev_url, wait_until="networkidle", timeout=45000)
                page.wait_for_timeout(300)
                html = page.content()
                soup = BeautifulSoup(html, "html.parser")
                t, d, s, e, l = parse_jsonld(soup)
                if not t:
                    # crude fallbacks
                    h1 = soup.select_one("h1,[data-testid='event-title']")
                    if h1:
                        t = h1.get_text(" ", strip=True)
                if not (s or e):
                    blk = soup.find(
                        lambda n: n.name in ("section", "div")
                        and "Date and time" in n.get_text(" ", strip=True)
                    )
                    if blk:
                        sdt, edt = parse_when(
                            blk.get_text(" ", strip=True), default_tz="America/New_York"
                        )
                        if sdt:
                            s = sdt.isoformat()
                        if edt:
                            e = edt.isoformat()
                if t and s:
                    out.append(
                        {
                            "title": t,
                            "description": d or "",
                            "link": ev_url,
                            "start": s,
                            "end": e,
                            "location": l,
                            "source": "eventbrite",
                        }
                    )
                    if os.getenv("FEEDS_DEBUG"):
                        LOG.debug(
                            "     · EB(PW) parsed: %s | start:%s loc:%s",
                            t[:60],
                            s,
                            (l or "")[:50],
                        )
                else:
                    if os.getenv("FEEDS_DEBUG"):
                        LOG.debug("   · EB(PW) skipped (missing title or start): %s", ev_url)
            except Exception as ex:
                if os.getenv("FEEDS_DEBUG"):
                    LOG.debug("   · EB(PW) detail error: %s %s", ev_url, str(ex)[:120])
                continue

        ctx.close()
        browser.close()

    return out


def fetch_eventbrite_discovery(list_url, pages=10, user_agent="fxbg-event-bot/1.0"):
    if not robots_allowed(list_url, user_agent):
        return []

    def _with_page(u: str, page_num: int) -> str:
        parts = list(urllib.parse.urlsplit(u))
        q = urllib.parse.parse_qs(parts[3])
        q["page"] = [str(page_num)]
        parts[3] = urllib.parse.urlencode({k: v[0] for k, v in q.items()})
        return urllib.parse.urlunsplit(parts)

    ua = os.getenv("EVENTBRITE_UA") or (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    )
    headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Upgrade-Insecure-Requests": "1",
    }

    detail_urls, pages_seen = set(), 0
    first_statuses = []

    for i in range(1, int(pages) + 1):
        u = _with_page(list_url, i)
        st, body, _ = req_with_cache(u, headers=headers, throttle=(1, 3))
        first_statuses.append(st)
        if os.getenv("FEEDS_DEBUG"):
            LOG.debug("   Eventbrite page %d: HTTP %s", i, st)
        if st != 200 or not body:
            continue
        pages_seen += 1
        soup = BeautifulSoup(body, "html.parser")
        for a in soup.select("a[href*='/e/']"):
            href = (a.get("href") or "").split("?", 1)[0]
            if not href:
                continue
            absu = urllib.parse.urljoin(u, href)
            path = urllib.parse.urlsplit(absu).path
            if not path.startswith("/e/"):
                continue
            if any(seg in path for seg in ("/organizer/", "/o/", "/collections/")):
                continue
            detail_urls.add(absu)
        if os.getenv("FEEDS_DEBUG"):
            LOG.debug("   Eventbrite (HTML): pages_visited=%d detail_urls=%d", pages_seen, len(detail_urls))

    if not detail_urls or all(s != 200 for s in first_statuses[:3]):
        if os.getenv("FEEDS_DEBUG"):
            LOG.debug("   Eventbrite (HTML) blocked or empty → falling back to Playwright")
        return fetch_eventbrite_discovery_playwright(list_url, pages=pages, user_agent=ua)

    out = []
    for ev_url in sorted(detail_urls):
        ev = _parse_eventbrite_detail(ev_url, user_agent=ua)
        if ev:
            out.append(ev)
        elif os.getenv("FEEDS_DEBUG"):
            LOG.debug("   · Eventbrite skipped (parse failed): %s", ev_url)
    return out


def fetch_rss(url, user_agent="fxbg-event-bot/1.0"):
    if not robots_allowed(url, user_agent):
        return []
    status, body, _ = req_with_cache(url, headers={"User-Agent": user_agent})
    if status == 304:
        LOG.debug("RSS %s -> 304 (no changes)", url)
        return []
    if status != 200 or not body:
        LOG.debug("RSS %s -> %s (no body)", url, status)
        return []
    feed = feedparser.parse(body)
    events = []
    for e in feed.entries:
        title = getattr(e, "title", "").strip()
        desc = getattr(e, "summary", "") or getattr(e, "description", "")
        link = getattr(e, "link", "")
        dt = None
        for k in ["start_time", "published", "updated", "created"]:
            if hasattr(e, k):
                try:
                    dt = parser.parse(getattr(e, k))
                    break
                except Exception:
                    pass
        events.append(
            {
                "title": title,
                "description": desc,
                "link": link,
                "start": dt.isoformat() if dt else None,
                "end": None,
                "location": None,
                "source": url,
            }
        )
    LOG.debug("RSS %s -> %d events", url, len(events))
    return events


def fetch_ics(url, user_agent="fxbg-event-bot/1.0"):
    if not robots_allowed(url, user_agent):
        return []
    status, body, _ = req_with_cache(url, headers={"User-Agent": user_agent})
    if status == 304 or status != 200 or not body:
        return []

    def ics_unescape(s: str) -> str:
        return (
            s.replace("\\n", "\n")
            .replace("\\N", "\n")
            .replace("\\,", ",")
            .replace("\\;", ";")
            .replace("\\\\", "\\")
        )

    def parse_ics_dt(s):
        if not s:
            return None
        s = s.replace("Z", "")
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
                if L.startswith(name_u + ":") or L.startswith(name_u + ";"):
                    return ln.split(":", 1)[1]
            return None

        title = get_prop("SUMMARY")
        loc = get_prop("LOCATION")
        dtstart = get_prop("DTSTART")
        dtend = get_prop("DTEND")
        desc = get_prop("DESCRIPTION")
        url_prop = get_prop("URL")

        if title:
            title = ics_unescape(title.strip())
        if loc:
            loc = ics_unescape(loc.strip())
        if desc:
            desc = ics_unescape(desc.strip())

        sdt = parse_ics_dt(dtstart)
        edt = parse_ics_dt(dtend)

        link = None
        if url_prop:
            try:
                link = urllib.parse.urljoin(url, url_prop.strip())
            except Exception:
                link = url_prop.strip()

        events.append(
            {
                "title": title,
                "description": desc,
                "link": link,
                "start": sdt.isoformat() if sdt else None,
                "end": edt.isoformat() if edt else None,
                "location": loc,
                "source": url,
            }
        )

    return events


# ---------- Fredericksburg Free Press scraper ----------
from dateutil import parser as dtparse, tz as dttz


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
            dt = dt.replace(tzinfo=dttz.gettz(default_tz))
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
    status, body, _ = req_with_cache(url, headers=headers, throttle=(2, 5))
    if status == 304:
        return []
    if status != 200:
        if os.getenv("FEEDS_DEBUG"):
            LOG.debug("   FreePress HTTP %s", status)
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
            end = _parse_dt(evt.get("endDate"), default_tz)
            url_e = evt.get("url") or url
            desc = _clean_text(evt.get("description", ""))

            loc_block = evt.get("location") or {}
            if isinstance(loc_block, dict):
                loc_name = loc_block.get("name") or ""
                addr = loc_block.get("address") or {}
                if isinstance(addr, dict):
                    addr_txt = " ".join(
                        filter(
                            None,
                            [
                                addr.get("streetAddress"),
                                addr.get("addressLocality"),
                                addr.get("addressRegion"),
                                addr.get("postalCode"),
                            ],
                        )
                    )
                else:
                    addr_txt = addr if isinstance(addr, str) else ""
                location = _clean_text(" - ".join([loc_name, addr_txt]).strip(" -"))
            else:
                location = _clean_text(str(loc_block))

            out.append(
                {
                    "title": name,
                    "description": desc,
                    "location": location,
                    "start": start.isoformat() if start else None,
                    "end": end.isoformat() if end else None,
                    "link": url_e,
                    "source": url,
                }
            )

        if isinstance(data, dict):
            if data.get("@type") == "Event":
                emit(data)
            for node in (data.get("@graph") or []):
                if isinstance(node, dict) and node.get("@type") == "Event":
                    emit(node)

        if isinstance(data, list):
            for node in data:
                if isinstance(node, dict) and node.get("@type") == "Event":
                    emit(node)

    if out:
        return out

    # ---------- 2) Microdata Events ----------
    for ev in soup.select(
        '[itemscope][itemtype*="schema.org/Event"], [itemscope][itemtype*="schema.org/event"]'
    ):
        def gp(prop):
            el = ev.select_one(f'[itemprop="{prop}"]')
            if not el:
                return None
            if el.has_attr("content"):
                return el["content"]
            if el.has_attr("datetime"):
                return el["datetime"]
            return el.get_text(" ", strip=True)

        title = _clean_text(gp("name") or gp("summary") or "")
        start = _parse_dt(gp("startDate") or gp("startTime"), default_tz)
        end = _parse_dt(gp("endDate") or gp("endTime"), default_tz)
        desc = _clean_text(gp("description") or "")
        loc_name = ""
        loc_el = ev.select_one('[itemprop="location"]')
        if loc_el:
            nm = loc_el.select_one('[itemprop="name"]')
            if nm:
                loc_name = nm.get_text(" ", strip=True)
            if not loc_name:
                loc_name = loc_el.get_text(" ", strip=True)
        link_el = ev.select_one("a[href]")
        href = link_el["href"] if link_el and link_el.has_attr("href") else url

        if title:
            out.append(
                {
                    "title": title,
                    "description": desc,
                    "location": _clean_text(loc_name),
                    "start": start.isoformat() if start else None,
                    "end": end.isoformat() if end else None,
                    "link": href,
                    "source": url,
                }
            )

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
        start_txt = None
        end_txt = None
        tstarts = node.select("time[datetime]")
        if tstarts:
            start_txt = tstarts[0].get("datetime")
            if len(tstarts) > 1:
                end_txt = tstarts[1].get("datetime")
        if not start_txt:
            dt_guess = node.get_text(" ", strip=True)
            m = re.search(
                r"\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)?\.?,?\s*[A-Z][a-z]+\.?\s*\d{1,2}[^|,]*\d{1,2}:\d{2}\s*(?:AM|PM|am|pm)?",
                dt_guess,
            )
            if m:
                start_txt = m.group(0)

        start = _parse_dt(start_txt, default_tz)
        end = _parse_dt(end_txt, default_tz)

        loc = ""
        loc_el = node.select_one(
            ".tribe-events-calendar-list__event-venue, .event-venue, .location"
        )
        if loc_el:
            loc = _clean_text(loc_el.get_text(" ", strip=True))

        desc_el = node.select_one(
            ".tribe-events-calendar-list__event-description, .entry-content, .event-description, p"
        )
        desc = _clean_text(desc_el.get_text(" ", strip=True)) if desc_el else ""

        if title and start:
            out.append(
                {
                    "title": title,
                    "description": desc,
                    "location": loc,
                    "start": start.isoformat(),
                    "end": end.isoformat() if end else None,
                    "link": href,
                    "source": url,
                }
            )

    return out


# ---------- Generic HTML fetcher with optional parser hint ----------
def fetch_html(url, hints=None, user_agent="fxbg-event-bot/1.0", throttle=(2, 5)):
    """
    When hints is a dict of CSS selectors (legacy behavior):
      hints = { 'item': '...', 'title': '...', 'date': '...', 'time': '...', 'location': '...', 'description': '...' }

    Or, you can pass a parser hint:
      hints = { 'parser': 'freepress', 'timezone': 'America/New_York' }
    """
    hints = hints or {}
    if not robots_allowed(url, user_agent):
        return []

    parser_name = (hints.get("parser") or "").lower()
    if "fredericksburgfreepress" in url or parser_name == "freepress":
        tzname = hints.get("timezone", "America/New_York")
        return fetch_freepress_calendar(url, default_tz=tzname)

    status, body, _ = req_with_cache(
        url, headers={"User-Agent": user_agent}, throttle=throttle
    )
    if status == 304:
        return []
    if status != 200 or not body:
        return []

    soup = BeautifulSoup(body, "html.parser")
    out = []

    css = hints
    items = soup.select(css.get("item")) if css.get("item") else []
    for el in items:
        time_node = el.select_one("time[datetime]") or el.find(
            "time", attrs={"datetime": True}
        )
        maybe_text = (el.get_text(" ", strip=True) or "").lower()
        looks_time = bool(
            re.search(r"\b\d{1,2}(:\d{2})?\s*(a\.m\.|am|p\.m\.|pm)\b", maybe_text)
        )
        if (not time_node) and (not looks_time):
            continue

        def pick(sel):
            if not sel:
                return None
            node = el.select_one(sel)
            return node.get_text(" ", strip=True) if node else None

        title = pick(css.get("title"))
        date_text = pick(css.get("date")) or pick(css.get("time"))
        loc = pick(css.get("location"))
        desc = pick(css.get("description"))
        s, e = parse_when(date_text)
        out.append(
            {
                "title": title,
                "description": desc,
                "link": None,
                "start": s.isoformat() if s else None,
                "end": e.isoformat() if e else None,
                "location": loc,
                "source": url,
            }
        )

    if not out:
        t = soup.select_one("h1, h2, .title")
        d = soup.select_one("time, .date, p")
        if t:
            s, e = parse_when(d.get_text(" ", strip=True) if d else None)
            out.append(
                {
                    "title": t.get_text(" ", strip=True),
                    "description": (
                        soup.select_one("body").get_text(" ", strip=True)[:500]
                        if soup.select_one("body")
                        else ""
                    ),
                    "link": url,
                    "start": s.isoformat() if s else None,
                    "end": e.isoformat() if e else None,
                    "location": None,
                    "source": url,
                }
            )

    return [e for e in out if e.get("title")]


def fetch_eventbrite(api_url, token_env=None):
    """
    Unified Eventbrite fetcher:
      - If `api_url` looks like an Eventbrite discovery or event HTML URL, use the HTML crawler.
      - Otherwise, use API path only if a token is provided.
    """
    if re.search(r"//[^/]*eventbrite\.com/(d/|e/)", api_url):
        if os.getenv("FEEDS_DEBUG"):
            LOG.debug("→ Eventbrite discovery crawl: %s", api_url)
        return fetch_eventbrite_discovery(api_url, pages=10)

    token = token_env or os.getenv("EVENTBRITE_TOKEN") or ""
    if not token:
        if os.getenv("FEEDS_DEBUG"):
            LOG.debug("   Eventbrite API: missing token (and URL is not discovery/detail); returning []")
        return []
    headers = {"Authorization": f"Bearer {token}"}
    status, body, _ = req_with_cache(api_url, headers=headers, throttle=(2, 5))
    if status == 304:
        return []
    if status != 200:
        if os.getenv("FEEDS_DEBUG"):
            LOG.debug("   Eventbrite HTTP %s", status)
            LOG.debug("%s", (body or "")[:200])
        return []
    try:
        data = json.loads(body)
        if os.getenv("FEEDS_DEBUG"):
            LOG.debug("   Eventbrite ok: top-level keys=%s", list(data.keys()))
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
        out.append(
            {
                "title": title,
                "description": desc,
                "link": ev.get("url"),
                "start": start,
                "end": end,
                "location": venue_name,
                "source": "eventbrite",
            }
        )
    return out


def fetch_bandsintown(url, app_id_env=None):
    if os.getenv("FEEDS_DEBUG"):
        LOG.debug(
            "   Bandsintown app_id present? %s",
            "YES" if (app_id_env or os.getenv("BANDSINTOWN_APP_ID")) else "NO",
        )
    app_id = app_id_env or os.getenv("BANDSINTOWN_APP_ID") or ""
    u = url.replace("${BANDSINTOWN_APP_ID}", app_id)
    if not app_id:
        if os.getenv("FEEDS_DEBUG"):
            LOG.debug("   Bandsintown missing app_id (empty)")
        return []
    status, body, _ = req_with_cache(u, headers={"User-Agent": "fxbg-event-bot/1.0"})
    if status == 304:
        return []
    if status != 200:
        if os.getenv("FEEDS_DEBUG"):
            LOG.debug("   Bandsintown HTTP %s", status)
            LOG.debug("%s", (body or "")[:200])
        return []
    try:
        data = json.loads(body)
        if os.getenv("FEEDS_DEBUG"):
            LOG.debug(
                "   Bandsintown ok: type=%s, count=%s",
                "list" if isinstance(data, list) else type(data).__name__,
                len(data) if isinstance(data, list) else len(data.get("events", [])),
            )
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
        out.append(
            {
                "title": title,
                "description": desc,
                "link": link,
                "start": start,
                "end": None,
                "location": location,
                "source": "bandsintown",
            }
        )
    return out

# ---- Public helper to resolve clean Eventbrite location from a detail URL ----
def resolve_eventbrite_location(detail_url: str) -> str:
    """
    Best-effort fetch of a single Eventbrite event page and return a clean
    location string (venue + address) using the same JSON-LD logic we use
    elsewhere. Returns '' if not found.
    """
    ev = _parse_eventbrite_detail(detail_url)
    loc = (ev or {}).get("location") if isinstance(ev, dict) else None
    return (loc or "").strip()

def _extract_dates_from_html(soup, default_tz="America/New_York"):
    """
    Return (iso_start, iso_end, date_text_fallback) where iso_* are ISO strings
    if available, else None. date_text_fallback is a human text block if found.
    """
    import json as _json
    iso_start = iso_end = None
    date_text = None

    # 1) JSON-LD @type=Event (also scans @graph)
    for tag in soup.select('script[type="application/ld+json"]'):
        try:
            data = _json.loads(tag.string or "")
        except Exception:
            continue

        def _events_in(obj):
            if isinstance(obj, dict):
                if obj.get("@type") in ("Event", "Festival"):
                    yield obj
                for node in (obj.get("@graph") or []):
                    if isinstance(node, dict) and node.get("@type") in ("Event", "Festival"):
                        yield node
            elif isinstance(obj, list):
                for node in obj:
                    if isinstance(node, dict) and node.get("@type") in ("Event", "Festival"):
                        yield node

        for evnode in _events_in(data):
            s = (evnode.get("startDate") or evnode.get("start_date") or "").strip()
            e = (evnode.get("endDate") or evnode.get("end_date") or "").strip()
            if s and not iso_start:
                iso_start = s
            if e and not iso_end:
                iso_end = e
            if iso_start and iso_end:
                break
        if iso_start or iso_end:
            break

    # 2) <time datetime="...">
    if not (iso_start or iso_end):
        ts = [t.get("datetime") for t in soup.select("time[datetime]") if t.get("datetime")]
        if ts:
            iso_start = ts[0]
            if len(ts) > 1:
                iso_end = ts[1]

    # 3) meta itemprop
    if not (iso_start or iso_end):
        m_start = soup.select_one("meta[itemprop='startDate'], meta[itemprop='startdate']")
        m_end   = soup.select_one("meta[itemprop='endDate'], meta[itemprop='enddate']")
        if m_start and m_start.get("content"):
            iso_start = (m_start["content"] or "").strip() or iso_start
        if m_end and m_end.get("content"):
            iso_end = (m_end["content"] or "").strip() or iso_end

    # 4) Visible block with date/time words
    if not (iso_start or iso_end):
        dt_blk = soup.find(
            lambda t: t and t.name in ("section", "div")
            and any(k in t.get_text(" ", strip=True) for k in ("Date", "Time", "When"))
        )
        if dt_blk:
            date_text = dt_blk.get_text(" ", strip=True)

    return iso_start, iso_end, date_text


def fetch_macaronikid_fxbg_playwright(days=60, user_agent=None, headless=True, save_artifacts=True):
    """
    Playwright crawler for Macaroni KID Fredericksburg.
    - Visits list pages (/events, /events/calendar, /events?page=1..8)
    - Collects detail links that look like real events
    - Prefers per-event .ics (including data: URIs) via fetch_ics()
    - Falls back to parsing HTML blocks (same logic as requests fallback)
    Returns raw event dicts to be normalized by normalize_event().
    """
    from playwright.sync_api import sync_playwright
    from urllib.parse import urlsplit, urlunsplit, urljoin
    from datetime import datetime
    import pathlib
    import re as _re

    log = logging.getLogger("sources")

    base = "https://fredericksburg.macaronikid.com"
    start_urls = [
        f"{base}/events",
        f"{base}/events/calendar",
        *[f"{base}/events?page={i}" for i in range(1, 9)],
    ]

    # Realistic UA unless overridden
    ua = user_agent or os.getenv("MAC_KID_UA") or (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    )

    def _detail_links(html, page_url):
        soup = BeautifulSoup(html, "html.parser")
        links = set()
        pat = _re.compile(r"^/events/[0-9a-f]{8,}(?:/[\w\-]*)?$", _re.I)
        for a in soup.select("a[href*='/events/']"):
            href = (a.get("href") or "").split("?", 1)[0].strip()
            if not href:
                continue
            absu = urljoin(page_url, href)
            try:
                p = urlsplit(absu).path
            except Exception:
                p = href
            if pat.match(p) and not p.rstrip("/").endswith("/events") and not p.rstrip("/").endswith("/events/calendar"):
                links.add(absu)
        return links

    def _slug(s):
        s = (s or "").lower()
        s = _re.sub(r"[^a-z0-9]+", "-", s).strip("-")
        return s[:80] or "event"

    out = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        ctx = browser.new_context(
            user_agent=ua,
            locale="en-US",
            timezone_id="America/New_York",
            viewport={"width": 1366, "height": 900},
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
        )
        page = ctx.new_page()

        detail_urls = set()
        for u in start_urls:
            try:
                page.goto(u, wait_until="networkidle", timeout=45000)
                html = page.content()
                new_links = _detail_links(html, u)
                detail_urls |= new_links
                logging.getLogger("sources").debug("MacKID(PW) %s -> added %d (cum=%d)", u, len(new_links), len(detail_urls))
            except Exception as ex:
                logging.getLogger("sources").warning("MacKID(PW) listing error on %s: %s", u, str(ex)[:160])

        if not detail_urls:
            logging.getLogger("sources").info("MacKID(PW) collected events: 0")
            ctx.close(); browser.close()
            return out

        debug_dir = pathlib.Path("data/debug")
        if save_artifacts:
            debug_dir.mkdir(parents=True, exist_ok=True)

        for ev_url in sorted(detail_urls):
            try:
                page.goto(ev_url, wait_until="domcontentloaded", timeout=45000)
                # Handle CF/JS challenge-ish pages
                title = (page.title() or "").strip()
                if any(k in title for k in ("Just a moment", "Attention Required", "Please Wait")):
                    logging.getLogger("sources").warning("MacKID(PW): challenge on detail, waiting… %s", ev_url)
                    page.wait_for_timeout(5000)
                    page.wait_for_load_state("networkidle", timeout=20000)

                page.wait_for_load_state("networkidle", timeout=30000)
                html = page.content()
                title = page.title() or ""

                # Save artifacts
                if save_artifacts and os.getenv("FEEDS_DEBUG"):
                    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
                    path_part = urlsplit(ev_url).path.strip("/").replace("/", "_")
                    stem = f"mackid_detail_{ts}__{path_part}_{_slug(title)}"
                    png = debug_dir / (stem + ".png")
                    htm = debug_dir / (stem + ".html")
                    try:
                        page.screenshot(path=str(png), full_page=True)
                    except Exception:
                        pass
                    try:
                        htm.write_text(html, encoding="utf-8")
                    except Exception:
                        pass
                    logging.getLogger("sources").debug("MacKID(PW) saved artifacts: %s , %s", png, htm)

                logging.getLogger("sources").debug("MacKID(PW) detail %s -> status=200 title=%r", ev_url, title)

                # Prefer per-event ICS link (can be HTTPS or data:)
                ics_href = None
                try:
                    # href endswith .ics
                    ics_href = page.eval_on_selector(
                        "a[href$='.ics']",
                        "el => el && el.getAttribute('href')"
                    )
                    if not ics_href:
                        # text mentions Apple Calendar
                        ics_href = page.eval_on_selector(
                            "a[href]",
                            "el => (el.textContent||'').toLowerCase().includes('apple calendar') ? el.getAttribute('href') : null"
                        )
                except Exception:
                    ics_href = None

                if ics_href:
                    ics_abs = urljoin(ev_url, ics_href)
                    try:
                        evs = fetch_ics(ics_abs, user_agent="fxbg-event-bot/1.0") or []
                        for e in evs:
                            e["source"] = "macaronikid"
                            e.setdefault("link", ev_url)
                        out.extend(evs)
                        logging.getLogger("sources").debug("MacKID(PW): ICS ok %s -> +%d", ics_abs, len(evs))
                        continue  # done with this detail page
                    except Exception as ex:
                        logging.getLogger("sources").warning("MacKID(PW) ICS parse failed %s: %s", ics_abs, str(ex)[:160])

                # ---- HTML fallback (mirrors fetch_macaronikid_fxbg) ----
                soup = BeautifulSoup(html, "html.parser")

                h = soup.select_one("h1") or soup.select_one("[data-element='event-title']")
                title_txt = h.get_text(" ", strip=True) if h else None

                d = soup.select_one("[data-element='event-description'], .article-content, .event-description")
                desc = d.get_text(" ", strip=True) if d else ""

                l = soup.select_one("[data-element='event-location'], .event-location, .location, [itemprop='location']")
                loc = l.get_text(" ", strip=True) if l else None

                # ---- robust date extraction
                iso_start, iso_end, date_text = _extract_dates_from_html(soup)
                combined_dt = None
                if iso_start or iso_end:
                    combined_dt = f"{iso_start or ''} {iso_end or ''}".strip()
                elif date_text:
                    combined_dt = date_text

                # DEBUG log instead of print
                if os.getenv("FEEDS_DEBUG"):
                    logging.getLogger("sources").debug(
                        "MacKID parsed: %s | date_text: %s",
                        (title_txt or "")[:80],
                        (combined_dt or "")[:120],
                    )

                sdt = edt = None
                if combined_dt:
                    sdt, edt = parse_when(combined_dt, default_tz="America/New_York")

                # If we STILL don't have a start, skip this event (don't emit broken VEVENTs)
                if not sdt:
                    if os.getenv("FEEDS_DEBUG"):
                        logging.getLogger("sources").debug("MacKID skip (no date): %s", ev_url)
                    continue

                out.append({
                    "title": title_txt or "(untitled)",
                    "description": desc or "",
                    "link": ev_url,
                    "start": sdt.isoformat(),
                    "end": (edt.isoformat() if edt else None),
                    "location": loc,
                    "source": "macaronikid",
                })



            except Exception as ex:
                logging.getLogger("sources").warning("MacKID(PW) detail error %s: %s", ev_url, str(ex)[:160])

        ctx.close(); browser.close()

    logging.getLogger("sources").info("MacKID(PW) collected events: %d", len(out))
    return out


def fetch_macaronikid_fxbg(days=60, user_agent=None):
    """
    Crawl Macaroni KID Fredericksburg using requests (no Playwright):
      - list view (/events?page=1..)
      - backup: /events and /events/calendar
      - mine real event links from anchors, JSON-LD, and regex
      - final fallback: crawl sitemap(s)
      - per-event: prefer .ics link (handles http(s) and data:), fallback to HTML dates
    Returns raw events to be normalized by normalize_event().
    """
    import json as _json
    from datetime import datetime, timedelta

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
    for i in range(1, 9):  # crawl up to 8 pages
        start_urls.append(f"{base}/events?page={i}")

    def _extract_links_from_jsonld(html, page_url):
        links = set()
        for tag in BeautifulSoup(html, "html.parser").select('script[type="application/ld+json"]'):
            try:
                data = _json.loads(tag.string or "")
            except Exception:
                continue
            seq = []
            if isinstance(data, dict):
                seq.append(data)
                seq.extend(data.get("@graph") or [])
            elif isinstance(data, list):
                seq.extend(data)
            for node in seq:
                if not isinstance(node, dict):
                    continue
                if node.get("@type") in ("Event", "Festival"):
                    u = (node.get("url") or "").strip()
                    if u:
                        links.add(urllib.parse.urljoin(page_url, u))
        return links

    def _extract_links_by_regex(html, page_url):
        """
        Scan entire HTML (including scripts) for event URLs like:
          /events/<hex>[/slug]
        Accept both absolute and relative.
        """
        links = set()
        pat = re.compile(
            r"https?://[^\"'\s]*?/events/[0-9a-f]{8,}(?:/[A-Za-z0-9\-_%]+)?|/events/[0-9a-f]{8,}(?:/[A-Za-z0-9\-_%]+)?",
            re.I,
        )
        for m in pat.finditer(html):
            href = m.group(0)
            links.add(urllib.parse.urljoin(page_url, href))
        return links

    def _crawl_sitemap(sitemap_url, acc, site_base, depth=0, max_depth=2):
        if depth > max_depth:
            return
        st, body, _ = req_with_cache(sitemap_url, headers={"User-Agent": "fxbg-event-bot/1.0"}, throttle=(1, 2))
        HTTP_LOG.debug("HTTP GET %s -> %s", sitemap_url, st)
        if st != 200 or not body:
            return
        soup = BeautifulSoup(body, "xml")
        # sitemap index?
        for loc in soup.select("sitemap > loc"):
            u = (loc.get_text() or "").strip()
            if u:
                _crawl_sitemap(u, acc, site_base, depth=depth + 1, max_depth=max_depth)
        # urlset
        for loc in soup.select("url > loc"):
            u = (loc.get_text() or "").strip()
            if u and urllib.parse.urlsplit(u).netloc.endswith(urllib.parse.urlsplit(site_base).netloc):
                acc.add(u)

    def _sitemap_event_links(site_base):
        found = set()
        try:
            parts = urllib.parse.urlsplit(site_base)
            robots_url = f"{parts.scheme}://{parts.netloc}/robots.txt"
            st, body, _ = req_with_cache(robots_url, headers={"User-Agent": "fxbg-event-bot/1.0"}, throttle=(1, 2))
            if st == 200 and body:
                for ln in body.splitlines():
                    if ln.lower().startswith("sitemap:"):
                        sm = ln.split(":", 1)[1].strip()
                        _crawl_sitemap(sm, found, site_base)
        except Exception as ex:
            LOG.debug("MacKID sitemap robots error: %s", str(ex)[:140])
        if not found:
            try:
                parts = urllib.parse.urlsplit(site_base)
                sm = f"{parts.scheme}://{parts.netloc}/sitemap.xml"
                _crawl_sitemap(sm, found, site_base)
            except Exception as ex:
                LOG.debug("MacKID sitemap direct error: %s", str(ex)[:140])
        evs = {u for u in found if "/events/" in u}
        LOG.debug("MacKID sitemap -> %d event URLs", len(evs))
        return evs

    def _get(url):
        headers = {
            "User-Agent": user_agent,
            "Referer": base + "/",
            "Accept-Language": "en-US,en;q=0.9",
        }
        status, body, headers_out = req_with_cache(url, headers=headers, throttle=(1, 3))
        return status, (body or ""), headers_out

    def _find_event_links(html, page_url):
        soup = BeautifulSoup(html, "html.parser")
        links = set()
        detail_pat = re.compile(r"^/events/[0-9a-f]{8,}(?:/[\w\-]*)?$", re.I)

        # 1) Normal anchors
        a_links = 0
        for a in soup.select("a[href*='/events/']"):
            href = (a.get("href") or "").split("?", 1)[0].strip()
            if not href:
                continue
            abs_url = urllib.parse.urljoin(page_url, href)
            path = urllib.parse.urlsplit(abs_url).path
            if detail_pat.match(path) and not path.rstrip("/").endswith("/events") and not path.rstrip("/").endswith(
                "/events/calendar"
            ):
                links.add(abs_url)
                a_links += 1

        # 2) JSON-LD Event URLs
        ld_links = _extract_links_from_jsonld(html, page_url)
        links |= ld_links

        # 3) Regex scan (includes script blobs)
        rx_links = _extract_links_by_regex(html, page_url)
        links |= rx_links

        LOG.debug(
            "MacKID links on %s -> anchors:%d jsonld:%d regex:%d total:%d",
            page_url,
            a_links,
            len(ld_links),
            len(rx_links),
            len(links),
        )
        return links

    detail_urls = set()
    pages_visited = 0
    max_pages = 20  # overall safety cap across all starts

    LOG.debug("MacKID: using requests/sitemap fallback …")
    for start in start_urls:
        if pages_visited >= max_pages:
            break
        st, body, _ = _get(start)
        if st == 200 and body:
            new_links = _find_event_links(body, start)
            detail_urls |= new_links
            pages_visited += 1
        else:
            LOG.debug("MacKID GET %s -> %s (len=%s)", start, st, len(body) if body else 0)

    if not detail_urls:
        sm_links = _sitemap_event_links(base)
        detail_urls |= sm_links

    if os.getenv("FEEDS_DEBUG"):
        LOG.info("MacKID: pages_visited=%d detail_urls=%d", pages_visited, len(detail_urls))
        for u in list(sorted(detail_urls))[:10]:
            LOG.debug("   · detail: %s", u)

    collected = []

    for ev_url in sorted(detail_urls):
        st, body, _ = _get(ev_url)
        if ev_url.rstrip("/").endswith("/events") or ev_url.rstrip("/").endswith("/events/calendar"):
            continue
        if st != 200 or not body:
            LOG.debug("MacKID detail GET %s -> %s", ev_url, st)
            continue
        soup = BeautifulSoup(body, "html.parser")

        # Prefer per-event ICS link (may be http(s) or data:)
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
                LOG.debug("MacKID ICS ok %s -> +%d", ics_href, len(collected))
                continue
            except Exception as ex:
                LOG.debug("MacKID ICS fetch failed %s (%s) → fallback to HTML", ics_href, str(ex)[:140])

        # HTML fallback
        title = None
        desc = None
        loc = None
        date_text = None

        h = soup.select_one("h1") or soup.select_one("[data-element='event-title']")
        if h:
            title = h.get_text(" ", strip=True)

        d = soup.select_one("[data-element='event-description'], .article-content, .event-description")
        if d:
            desc = d.get_text(" ", strip=True)

        l = soup.select_one("[data-element='event-location'], .event-location, .location, [itemprop='location']")
        if l:
            loc = l.get_text(" ", strip=True)

        # structured date/time blocks
        nodes = soup.select("[data-element='event-date'], .event-date, .event-time")
        if nodes:
            date_text = " ".join(n.get_text(" ", strip=True) for n in nodes if n.get_text(strip=True))

        # ISO datetimes from <time datetime="...">
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

        # itemprop meta
        if not date_text:
            sm_tag = soup.select_one("meta[itemprop='startDate'], meta[itemprop='startdate']")
            em_tag = soup.select_one("meta[itemprop='endDate'], meta[itemprop='enddate']")
            sm = sm_tag.get("content").strip() if sm_tag and sm_tag.get("content") else ""
            em = em_tag.get("content").strip() if em_tag and em_tag.get("content") else ""
            if sm or em:
                date_text = f"{sm} {em}".strip()

        # JSON-LD Event (object OR list)
        if not date_text:
            for s in soup.find_all("script", type="application/ld+json"):
                try:
                    dct = _json.loads(s.string)
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

        sdt, edt = parse_when(date_text or "", default_tz="America/New_York")
        collected.append(
            {
                "title": title or "(untitled)",
                "description": desc or "",
                "link": ev_url,
                "start": sdt.isoformat() if sdt else None,
                "end": (edt.isoformat() if edt else None) if edt else None,
                "location": loc,
                "source": "macaronikid",
            }
        )

    LOG.info("MacKID collected events: %d", len(collected))
    return collected
