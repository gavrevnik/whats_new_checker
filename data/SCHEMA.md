# SQLite schema

The active datastore is `data/library.sqlite3`. The application initializes missing tables from `app/storage.py`; legacy CSV snapshots are preserved in `legacy/csv/` and are not read at runtime.

## Core tables

- `content_types`: extensibility point for future content. Only `movie` is enabled.
- `content_items`: bilingual titles, workflow status (`backlog`, `consumed`, `dismissed`), reaction (``, `like`, `dislike`), provenance, notes, original user input in `raw_json`, and timestamps.
- `movies`: release date/year, runtime, separate IMDb, Kinopoisk, and TMDB ratings, vote counts, IMDb/Kinopoisk/TMDB IDs, overview, language, awards, tagline, certification, Metascore, box office, extended details JSON, and last TMDB refresh time.
- `people`: canonical bilingual names, TMDB IDs, original user input in `raw_json`, extended TMDB details JSON, and last refresh time.
- `interest_roles`: active actors/directors used for recommendations.
- `movie_people`: directors and matched interest people for each movie.
- `genres` / `movie_genres`: normalized movie genres.
- `content_aliases`: aliases and external mappings used for deduplication.
- `trash_entries`: reversible soft-delete markers for a movie or a person role, plus a compact display snapshot. Active queries exclude marked entities; their normalized data and relationships remain intact.

`awards_json` is a JSON array. A summary-only provider uses:

```json
[{"source":"omdb","summary":"Won 2 Oscars. 15 wins & 30 nominations total"}]
```

A future structured festival provider can add objects such as:

```json
[{"event":"Cannes Film Festival","award":"Palme d'Or","result":"winner","year":2025,"source_url":"https://..."}]
```

TMDB does not expose structured awards. TMDB refresh therefore preserves existing awards unless an auxiliary provider returns new data.

`kinopoisk_id` and `kinopoisk_rating` are filled by Kinopoisk API Unofficial during TMDB enrichment. The provider lookup uses the movie title and a ±1 year window because its IMDb mapping and canonical year may be absent or differ from TMDB; IMDb ID is used as an additional exact match when present in the response. A saved non-empty rating is treated as cached and is not requested again. The public Kinopoisk URL is derived as `https://www.kinopoisk.ru/film/{kinopoisk_id}/`.

`movies.details_json` stores optional provider fields that should not expand the main table: cast, writers, countries, production companies, spoken languages, keywords, budget, revenue, homepage, poster URL, and provider status. The HTTP API flattens these fields for the movie detail card.
