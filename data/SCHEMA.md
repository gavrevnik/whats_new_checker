# SQLite schema

The active datastore is `data/library.sqlite3`. The application initializes missing tables from `app/storage.py`; legacy CSV snapshots are preserved in `legacy/csv/` and are not read at runtime.

## Core tables

- `content_types`: extensibility point for future content. Only `movie` is enabled.
- `content_items`: bilingual titles, workflow status (`backlog`, `consumed`, `dismissed`), reaction (``, `like`, `dislike`), provenance, notes, and timestamps.
- `movies`: release date/year, runtime, separate IMDb and TMDB ratings, vote counts, IMDb/TMDB IDs, overview, language, awards, tagline, certification, Metascore, box office, extended details JSON, and last TMDB refresh time.
- `people`: canonical bilingual names, TMDB IDs, extended TMDB details JSON, and last refresh time.
- `interest_roles`: active actors/directors used for recommendations.
- `movie_people`: directors and matched interest people for each movie.
- `genres` / `movie_genres`: normalized movie genres.
- `content_aliases`: aliases and external mappings used for deduplication.

`awards_json` is a JSON array. A summary-only provider uses:

```json
[{"source":"omdb","summary":"Won 2 Oscars. 15 wins & 30 nominations total"}]
```

A future structured festival provider can add objects such as:

```json
[{"event":"Cannes Film Festival","award":"Palme d'Or","result":"winner","year":2025,"source_url":"https://..."}]
```

TMDB does not expose structured awards. TMDB refresh therefore preserves existing awards unless an auxiliary provider returns new data.

`movies.details_json` stores optional provider fields that should not expand the main table: cast, writers, countries, production companies, spoken languages, keywords, budget, revenue, homepage, poster URL, and provider status. The HTTP API flattens these fields for the movie detail card.
