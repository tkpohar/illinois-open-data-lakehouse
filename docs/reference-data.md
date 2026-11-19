# Reference data (seeds)

Small, slowly changing lookup tables live as CSV files in [`seeds/`](../seeds) and are
loaded into the `reference` schema with:

```bash
lakehouse seed            # load new or changed seeds
lakehouse seed --force    # reload everything
```

```
reference.community_areas: 77 rows (loaded, sha256 3b1f0c2a9e4d)
reference.police_districts: 22 rows (unchanged, sha256 9c0d1e2f3a4b)
```

| Table | File | Key | Rows | Contents |
|---|---|---|---|---|
| `reference.community_areas` | `seeds/community_areas.csv` | `community_area` (1–77) | exactly 77 | official community-area numbers and names |
| `reference.police_districts` | `seeds/police_districts.csv` | `district` (1–25) | 22 | current Chicago Police Department districts and names |

## How loading works

- **Validate first.** Every seed is parsed and checked before the database is touched:
  exact header, no empty values, integer keys, unique keys, keys within range, and the
  expected row count where one is declared. One bad seed fails the whole run
  (`SeedError`, exit status 1), and the existing tables are left as they were.
- **Load atomically.** Each table is recreated with a `PRIMARY KEY` and filled in one
  transaction.
- **Skip what didn't change.** The SHA-256 of each file is recorded in
  `reference._seed_log` (`seed, loaded_at, rows, checksum`). An unchanged file is
  skipped unless `--force` is given.

Seeds are declared in `lakehouse.reference.SEEDS` (columns and types, key, key range,
expected rows). To add one, add a CSV and an entry there.

## Using it

Silver tables store community area and district as integers so they join directly:

```sql
SELECT ca.name AS community_area, count(*) AS crimes
FROM silver.crimes c
JOIN reference.community_areas ca USING (community_area)
GROUP BY 1 ORDER BY 2 DESC;
```

Referential-integrity checks from silver to these tables come in Milestone 28. Every
load also records changes in `reference.<seed>_history` ([SCD Type 2](scd2.md)).

## Provenance and caveats

- **Community areas.** The 77 numbered areas have been stable since the 1980s (O'Hare
  became 76 and Edgewater 77). Names use the city's usual spelling.
- **Police districts.** These are the 22 districts in use today. Districts 13, 21 and 23
  were merged into neighbouring districts in 2012 and are **not** in the seed. Older crime
  records that still carry those numbers, or the code 31, will show up as unmatched in
  the integrity checks rather than being given guessed names.
- The seeds were compiled from public City of Chicago and CPD information without live
  access to the data portal. Before relying on them for publication, compare them with
  the portal's *Boundaries – Community Areas* and *Boundaries – Police Districts*
  datasets.
