# FXBG Event Feeds — auto-updating iPhone calendars (Family, Adult, Recurring)

This repo scrapes **Fredericksburg-area** event sources (official calendars, venue pages, kid calendars, breweries)
and builds **three live .ics feeds** you can subscribe to on your iPhone:

- Family/Kids: `https://<your-username>.github.io/fxbg-event-feeds/family.ics`
- Adults/Nightlife: `https://<your-username>.github.io/fxbg-event-feeds/adult.ics`
- Recurring Deals & Events: `https://<your-username>.github.io/fxbg-event-feeds/recurring.ics`

## Install & Deploy (one time)

1) **Create GitHub repo** (e.g., `fxbg-event-feeds`) and upload all files.  
2) **Enable Pages** → Settings → Pages → Branch: `main` → Folder: `/docs`.  
3) **(Optional) Facebook**: add `FACEBOOK_TOKEN` to Repo → Settings → Secrets and variables → Actions → New Repository Secret.  
4) Edit `config.yaml` if you want to add/remove sources.  
5) Push. The GitHub Action:
   - runs daily (morning ET) and on each push,
   - scrapes sources,
   - dedupes/normalizes,
   - writes `docs/*.ics`,
   - commits changes.

## Subscribe on iPhone (one time)

**Settings → Calendar → Accounts → Add Subscribed Calendar** and paste each URL above.

## Local dev

```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python src/main.py
```

Outputs go in `docs/`. You can import `.ics` into any calendar app to test.

## Notes on Facebook / Terms

- This project ships **optional** Facebook Graph fetch (public Pages and events) if you supply a `FACEBOOK_TOKEN`.  
- It **does not** scrape private/closed groups or log-in-gated content. Respect each site’s Terms.  
- Prefer official sites, RSS/ICS, and public Pages.

## How categorization works

- **family**: kid/family words, farms, markets, festivals, library, pumpkins, etc.  
- **adult**: happy hour, tavern, brewery, live music, wine/cider, theatre, etc.  
- **recurring**: “every”, “weekly”, weekday names, “first/third” patterns.

You can tune these in `config.yaml`.
