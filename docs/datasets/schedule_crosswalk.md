# `schedule_crosswalk`

`schedule_crosswalk` reshaper -- release tag [`mbb_crosswalk`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mbb_crosswalk).

| | |
|---|---|
| **Builder** | [`python/espn_mbb_12_schedule_crosswalk_creation.py`](../../python/espn_mbb_12_schedule_crosswalk_creation.py) |
| **Release tag** | [`mbb_crosswalk`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mbb_crosswalk) |
| **File stem** | `mbb_schedule_crosswalk_{season}.{parquet,csv,rds}` |
| **Manifested** | yes |
| **Last published** | 2026-08-12 (newest release asset) |
| **Tag created** | 2026-06-13 |
| **Release assets** | 25 |

## Automation

`.github/workflows/daily_mbb.yml` -- daily cron, running `scripts/daily_mbb_data_processor.sh` (the single entrypoint). `-l python` is the default and builds via `mbb_data_build`; `-l R` is the retained rollback path over the R creation scripts.

## Columns

| col_name | type | description |
|---|---|---|
| `season` | Int32 | |
| `game_date` | Date | |
| `home_espn_team_id` | Int32 | |
| `away_espn_team_id` | Int32 | |
| `espn_game_id` | String | |
| `bart_muid` | String | |
| `bart_team1` | String | |
| `bart_team2` | String | |
| `bart_winner` | String | |
| `kp_game_id` | String | |
| `fox_game_id` | String | |
| `yahoo_game_id` | String | |
| `match_method` | String | |
| `match_confidence` | Float64 | |

## Coverage

| season | rows |
|---:|---:|
| 2026 | 6,386 |
