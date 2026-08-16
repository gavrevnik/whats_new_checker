# Deduplication rules

Use this precedence order:

1. Same `movies.tmdb_id` means the same item.
2. Same normalized original title and year means the same item.
3. Same normalized Russian title and year means the same item.
4. An alias in `content_aliases` means the same item as its `content_id`.

Normalize with Unicode case folding and removal of punctuation/spacing. Do not merge remakes, different cuts, or adaptations solely because their normalized titles match. When the year/version is missing and more than one plausible film exists, ask the user to disambiguate.
