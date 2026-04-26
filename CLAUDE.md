# CLAUDE.md — `yl-hb-imdb` (IMDb scraper)

Conventions shared across the `yl-hb-*` fleet live in
[`SCRAPER-CLAUDE-TEMPLATE.md`](../SCRAPER-CLAUDE-TEMPLATE.md) — read both.

> **Not to be confused with `yl-hb-imdbp`** (IMDb**Pro**, behind a login).
> This repo scrapes the **public** IMDb site and is much simpler.

## What this repo does

Scrapes public IMDb pages for:

1. **Profile enrichment** (`imdb-enrichment`) — fills `hb_talent`
   biography, headshot, country, etc. from `/name/nm…` pages.
2. **News scraping** (`imdb-news-scraper`) — pulls recent IMDb News
   articles into `public.news`.
3. **Top news** (`imdb-top-news`) — daily ingest of the top-news feed.

Also has an Airtable companion script (`enrich_imdb_airtable.py`) for
a parallel Airtable mirror of selected fields.

## Stack

**Browser-scraper** variant: Node 20, TypeScript via `ts-node`,
`puppeteer-extra` + stealth plugin. Service-role Supabase. Plus a
standalone Python Airtable script.

## Repo layout

```
src/
  …                                  # TS scrapers (run by the YAML workflows)
  supabase.ts                        # service-role client
enrich_imdb_airtable.py              # Python — Airtable mirror, separate flow
.github/workflows/
  imdb-enrichment.yml
  imdb-news-scraper.yml
  imdb-top-news.yml
package.json
tsconfig.json
```

## Supabase auth

Standard fleet convention — `SUPABASE_URL` + `SUPABASE_SERVICE_KEY` in
`src/supabase.ts`.

## Workflow lifecycle convention

All three TS workflows call `log_workflow_run` start + result with
hardcoded GitHub workflow ids matching `public.workflows.github_workflow_id`.

## Tables this repo touches

| Table | Operation | Notes |
|---|---|---|
| `public.hb_talent` | UPSERT | Person pages — biography, image, country. |
| `public.hb_socials` | UPSERT | External-link blocks on IMDb name pages. |
| `public.hb_media` | UPSERT | Title metadata where joined. |
| `public.news` | UPSERT | News + top-news feeds. |
| `public.countries` | SELECT (lookup) | Birth-country code resolution. |
| `public.workflows` | RPC `log_workflow_run` | Lifecycle reporting. |

## Running locally

```bash
npm install
npx puppeteer browsers install chrome
cp .env.example .env.local            # if present
# Set: SUPABASE_URL, SUPABASE_SERVICE_KEY, RAPIDAPI_KEY (some flows),
#      LIMIT, MAX_PAGES, CONCURRENCY, STALE_DAYS, SLEEP_MS, WORKFLOW_ID
npx ts-node --transpile-only src/<entry>.ts
```

For the Python Airtable mirror:

```bash
pip install -r requirements.txt       # if a top-level one exists; otherwise the script lists deps
export AIRTABLE_API_KEY=...
python3 enrich_imdb_airtable.py --limit 50
```

## Per-repo gotchas

- **Public IMDb only.** This repo does **not** require an IMDbPro
  login. If a scraper here starts asking for cookies, you've drifted
  into IMDbPro territory — move that work into `yl-hb-imdbp` instead.
- **Plain `puppeteer` is blocked.** Use `puppeteer-extra` + stealth
  (already in deps). When IMDb tightens detection, bump
  `puppeteer-extra-plugin-stealth` first.
- **`STALE_DAYS=30` is the default re-enrichment threshold.** Don't
  lower it below ~14 days without a corresponding RapidAPI / proxy
  budget bump.
- **`public.news` is shared with sibling scrapers** (`yl-hb-am`,
  `yl-hb-rgm`, `yl-hb-imdbp`, `yl-hb-tmdb`). Coordinate slug /
  `(source_domain, source_url)` uniqueness changes.
- **Python and TS flows touch overlapping fields** (the Airtable mirror
  shadows part of `hb_talent`). They don't share a model — coordinate
  changes.

## Conventions Claude should follow when editing this repo

- **Use `puppeteer-extra` with stealth.** Don't switch to bare puppeteer.
- **Hardcode the GitHub workflow id in YAML** matching this repo's
  local convention.
- **Don't pull IMDbPro-only fields here** — keep public/IMDbPro split.

## Related repos

- `yl-hb-imdbp` — sibling repo for the **logged-in IMDbPro** scraper.
- `yl-hb-am`, `yl-hb-rgm`, `yl-hb-tmdb`, `yl-hb-imdbp` — also write
  to `public.news`.
- `hb_app_build` — Next.js app reading the data.
