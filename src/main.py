# src/main.py

import re
import uuid
from datetime import datetime, timezone

# ics.py preferred
try:
    from ics import Calendar, Event
    try:
        # old ics.py
        from ics.grammar.parse import ContentLine  # type: ignore
    except Exception:
        # new ics.py
        from ics.grammar.line import ContentLine  # type: ignore
    HAS_ICS = True
except ImportError:
    HAS_ICS = False

# icalendar fallback (if you aren't using ics.py)
if not HAS_ICS:
    from icalendar import Calendar, Event  # type: ignore
    ContentLine = None  # sentinel

try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None

# ---------- helpers ----------

DATE_PREFIX_RE = re.compile(r"^[A-Za-z]{3}\s+\d{1,2},\s+\d{4}:\s+")
TRAILING_AT_RE = re.compile(r"\s+at\s+(.+)$", re.IGNORECASE)
HTML_TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"[ \t\f\v]+")

def strip_html_to_text(html: str) -> str:
    """Return compact RFC5545-safe plain text."""
    if not html:
        return ""
    if BeautifulSoup:
        txt = BeautifulSoup(html, "html.parser").get_text("\n")
    else:
        txt = HTML_TAG_RE.sub("", html).replace("&nbsp;", " ").replace("&amp;", "&")
    # normalize whitespace and blank lines
    lines = [WS_RE.sub(" ", ln).strip() for ln in txt.splitlines()]
    lines = [ln for ln in lines if ln]
    # ICS values should contain literal \n for newlines
    return "\\n".join(lines)

def add_html_alt_desc(event_obj, html: str):
    """Add X-ALT-DESC;FMTTYPE=text/html for clients that support it."""
    if not html:
        return
    if ContentLine:  # ics.py path
        event_obj.extra.append(
            ContentLine(
                name="X-ALT-DESC",
                params={"FMTTYPE": "text/html"},
                value=html,
            )
        )
    else:  # icalendar path
        event_obj.add("X-ALT-DESC", html, parameters={"FMTTYPE": "text/html"})

def clean_title_and_location(raw_title: str, existing_location: str | None) -> tuple[str, str | None]:
    """Remove 'Mon dd, yyyy: ' prefix and pull trailing ' at Location' into LOCATION."""
    title = (raw_title or "").strip()

    # Strip leading "Oct 23, 2025: "
    title = DATE_PREFIX_RE.sub("", title).strip()

    loc = (existing_location or "").strip()

    # If no explicit location, try to peel off ' at XYZ' at the end of the title
    if not loc:
        m = TRAILING_AT_RE.search(title)
        if m:
            loc = m.group(1).strip()
            title = TRAILING_AT_RE.sub("", title).strip()

    return title, (loc or None)

def coerce_dt(val):
    """Accept aware datetimes or ISO strings. If naive dt, force UTC."""
    if isinstance(val, datetime):
        return val if val.tzinfo else val.replace(tzinfo=timezone.utc)
    # ics.py accepts certain string formats; ISO works fine.
    return val

# ---------- core: build event + calendar ----------

def build_event(item) -> Event:
    e = Event()

    # SUMMARY (title only)
    raw_title = item.get("title") or item.get("name") or ""
    raw_loc = item.get("location") or ""
    title, loc = clean_title_and_location(raw_title, raw_loc)
    if title:
        # ics.py uses .name for SUMMARY
        try:
            e.name = title
        except Exception:
            e.summary = title  # icalendar path
    if loc:
        e.location = loc

    # DESCRIPTION (text) + X-ALT-DESC (html)
    desc_html = item.get("description_html") or item.get("description") or ""
    desc_text = strip_html_to_text(desc_html)
    if desc_text:
        e.description = desc_text
    if desc_html:
        add_html_alt_desc(e, desc_html)

    # DATES
    start = coerce_dt(item["start"])
    end = coerce_dt(item["end"])
    e.begin = start
    e.end = end

    # DTSTAMP-ish metadata
    now_utc = datetime.now(timezone.utc)
    try:
        e.created = now_utc
        e.last_modified = now_utc
    except Exception:
        pass

    # UID (stable)
    uid = (item.get("uid") or "").strip()
    if not uid:
        src_key = (item.get("id") or item.get("url") or f"{title}|{start}|{loc}").strip()
        uid = str(uuid.uuid5(uuid.NAMESPACE_URL, src_key))
    try:
        e.uid = uid
    except Exception:
        pass

    return e

def build_calendar(items) -> Calendar:
    cal = Calendar()
    try:
        cal.scale = "GREGORIAN"
        cal.method = None
        cal.extra = []  # ensure field exists on some ics.py versions
        # Ensure a stable PRODID if you want:
        cal.creator = "fxbg-event-feeds"
    except Exception:
        pass
    for it in items:
        cal.events.add(build_event(it))
    return cal

def write_calendar(items, out_path: str):
    cal = build_calendar(items)
    # Write with LF newlines and streaming fold
    with open(out_path, "w", encoding="utf-8", newline="\n") as f:
        try:
            # ics.py
            f.writelines(cal.serialize_iter())
        except AttributeError:
            # icalendar fallback
            f.write(cal.to_ical().decode("utf-8"))

# ---------- CLI entry (adapt to your project) ----------

def fetch_items_somehow():
    """
    Replace with your actual collector. Must return dicts like:
    {
      "title": "University of Mary Washington Women's Basketball vs ...",
      "start": datetime(..., tzinfo=timezone.utc) or ISO string,
      "end":   datetime(..., tzinfo=timezone.utc) or ISO string,
      "location": "Salisbury, MD",
      "description_html": "<p>…</p>",
      "url": "https://example",
      "uid": "optional-stable-uid"
    }
    """
    raise NotImplementedError

def main():
    items = fetch_items_somehow()
    write_calendar(items, "feed.ics")

if __name__ == "__main__":
    main()
