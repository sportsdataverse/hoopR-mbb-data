# `player_crosswalk`

`player_crosswalk` reshaper -- release tag [`mbb_crosswalk`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mbb_crosswalk).

| | |
|---|---|
| **Builder** | [`python/espn_mbb_13_player_crosswalk_creation.py`](../../python/espn_mbb_13_player_crosswalk_creation.py) |
| **Release tag** | [`mbb_crosswalk`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mbb_crosswalk) |
| **File stem** | `mbb_player_crosswalk_{season}.{parquet,csv,rds}` |
| **Manifested** | no (no load_mbb_*_manifest() loader yet) |
| **Last published** | — (newest release asset) |
| **Tag created** | — |
| **Release assets** | — |

## Automation

`.github/workflows/daily_mbb.yml` -- daily cron, running `scripts/daily_mbb_R_processor.sh` (R, the current daily producer). `scripts/daily_mbb_python_processor.sh` runs the parity-tested Python build in `mbb_data_build` but does not yet drive daily CI.

## Columns

| col_name | type | description |
|---|---|---|
| `season` | Int32 | |
| `espn_team_id` | Int32 | |
| `team_abbreviation` | String | |
| `player_name` | String | |
| `espn_athlete_id` | String | |
| `espn_full_name` | String | |
| `espn_jersey` | String | |
| `espn_position` | String | |
| `fox_athlete_id` | String | |
| `fox_player` | String | |
| `fox_jersey` | String | |
| `fox_position_group` | String | |
| `yahoo_player_id` | String | |
| `yahoo_player_name` | String | |
| `match_method` | String | |
| `match_confidence` | Float64 | |
| `match_keys` | String | |

## Coverage

| season | rows |
|---:|---:|
| 2026 | 5,442 |
