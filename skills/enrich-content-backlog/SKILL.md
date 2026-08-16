---
name: enrich-content-backlog
description: Find, filter, rank, and deduplicate movie recommendations against this repository's SQLite library. Use for requests to recommend, discover, refresh, or expand the film backlog, especially when constraints such as IMDb/TMDB ratings, duration, release dates, actors, directors, genres, or vote counts are supplied.
---

# Enrich Content Backlog

Build film recommendations from verified facts while treating `data/library.sqlite3` as both the exclusion list and the source of actor/director interests.

## Workflow

1. Run `python3 skills/enrich-content-backlog/scripts/export_context.py` from the repository root. Use its output to understand existing movies and active interests.
2. Translate the user's wording into explicit hard filters, soft preferences, result count, and sort order. Preserve exact inequalities: `>` and `>=` are not interchangeable.
3. Ask one concise question only when a missing value materially changes the search. Otherwise use the defaults below and state them.
4. Verify volatile ratings, release status, dates, runtime, and awards against current sources instead of guessing.
5. Exclude candidates matching an existing `movies.tmdb_id`, normalized original/Russian title plus adjacent release year, or `content_aliases` row.
6. Return candidates with both names, requested facts, and source links. Keep factual fields separate from the recommendation rationale.
7. Do not modify SQLite unless the user also asks to add results. For addition, use `$add-content-item`.

## Movie defaults

- Interpret an unspecified rating threshold as IMDb rating, not TMDB rating.
- Require the film to be released when the user asks what to watch now.
- Prefer active actors and directors from `interest_roles` when no people are supplied.
- Default sort: rating descending, then rating count descending.
- For the legacy request style, use IMDb > 6, release year >= 2020, runtime > 100 minutes, and 30 results.

## Output contract

Return a compact Markdown table unless the user specified another format. Include Russian/original title, release date, director, IMDb rating, genres, runtime, awards, matched interest people, TMDB ID, source URLs, and a one-line preference match. Treat TMDB and IMDb ratings as distinct fields. Call out an unverifiable field instead of weakening a filter silently.

Read [references/deduplication.md](references/deduplication.md) when titles have alternate spellings, remakes, editions, or translations.
