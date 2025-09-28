import hashlib, re
from datetime import datetime, timedelta
from dateutil import parser, tz

WEEKDAY_WORDS = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"]

def hash_event(title, start, location):
    base = f"{(title or '').strip()}|{start.isoformat()}|{(location or '').strip()}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()

def parse_when(text, default_tz="America/New_York", fallback_hours=2):
    """
    Best-effort parse for free-text date/time ranges like:
    "Sept 27, 6–9pm" or "September 27 7:30pm" or "10:00 to 5:00 pm"
    Returns (start_dt, end_dt) in local tz if possible, else (None, None).
    """
    if not text:
        return None, None
    text = re.sub(r"\s+", " ", str(text)).strip()
    parts = re.split(r"\s*[–\-to]+\s*", text, maxsplit=1, flags=re.IGNORECASE)
    local = tz.gettz(default_tz)
    try:
        start = parser.parse(parts[0], fuzzy=True, default=datetime.now())
        if not start.tzinfo:
            start = start.replace(tzinfo=local)
        end = None
        if len(parts) > 1:
            try:
                end = parser.parse(parts[1], fuzzy=True, default=start)
                if not end.tzinfo:
                    end = end.replace(tzinfo=local)
            except Exception:
                end = None
        if start and not end:
            end = start + timedelta(hours=fallback_hours)
        return start, end
    except Exception:
        return None, None

def categorize_text(title, desc, rules):
    text = f"{title or ''} {desc or ''}".lower()
    # recurring first
    for k in rules.get('recurring', []):
        if k in text:
            return 'recurring'
    for k in rules.get('family', []):
        if k in text:
            return 'family'
    for k in rules.get('adult', []):
        if k in text:
            return 'adult'
    # heuristics
    if any(w in text for w in WEEKDAY_WORDS) and ("every" in text or "weekly" in text):
        return 'recurring'
    return 'adult'
