---
name: add-content-item
description: Parse a natural-language request and add or update one movie in this repository's SQLite library with normalized Russian and original titles. Use when the user says to add, save, remember, watch, mark watched, like, or dislike a film.
---

# Add Content Item

Resolve the requested film, guard against ambiguity and duplicates, then write one normalized record through the repository storage layer.

## Workflow

1. Infer title, status, and reaction from the user's phrase.
   - Future intent such as “посмотреть” means `status=backlog`.
   - Completed intent means `status=consumed`.
   - “Понравилось” and “не понравилось” set `reaction=like` or `dislike` and normally imply `status=consumed`.
2. Resolve both `title_original` and `title_ru`. Use an established localized release title. If none exists, use a conventional transliteration or repeat the original.
3. Ask a concise disambiguating question for multiple plausible films, years, or versions. “Сияние” may refer to the 1980 film or a book; the active library accepts only movies.
4. Search `data/library.sqlite3` for TMDB ID, both normalized titles, aliases, and adjacent year. Update an existing movie rather than creating a duplicate.
5. Leave unknown fields empty. Treat IMDb and TMDB ratings separately; store awards as the JSON array defined in `data/SCHEMA.md`.
6. Run `scripts/add_item.py` with resolved fields. Report the ID and normalized titles.

## Command

```bash
python3 skills/add-content-item/scripts/add_item.py \
  --title-original "The Shining" \
  --title-ru "Сияние" \
  --year 1980 \
  --director "Stanley Kubrick" \
  --status backlog
```

Use `--dry-run` to validate without writing. Only movies are currently enabled; do not add music, restaurants, books, or news to the active database.
