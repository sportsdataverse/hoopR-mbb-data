# `officials`

`officials` reshaper -- release tag [`espn_mens_college_basketball_officials`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_officials).

| | |
|---|---|
| **Builder** | [`python/espn_mbb_10_officials_creation.py`](../../python/espn_mbb_10_officials_creation.py) |
| **Release tag** | [`espn_mens_college_basketball_officials`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_officials) |
| **File stem** | `officials_{season}.{parquet,csv,rds}` |
| **Manifested** | yes |
| **Last published** | — (newest release asset) |
| **Tag created** | — |
| **Release assets** | — |

## Automation

`.github/workflows/daily_mbb.yml` -- daily cron, running `scripts/daily_mbb_data_processor.sh` (the single entrypoint). `-l python` is the default and builds via `mbb_data_build`; `-l R` is the retained rollback path over the R creation scripts.

## Columns

| col_name | type | description |
|---|---|---|
| `season` | Int32 | |
| `game_id` | Int32 | |
| `official_full_name` | String | |
| `official_display_name` | String | |
| `official_position` | String | |
| `official_position_id` | Int32 | |
| `official_order` | Int32 | |

## Coverage

| season | rows |
|---:|---:|
| 2025 | 18,284 |
| 2026 | 18,483 |
