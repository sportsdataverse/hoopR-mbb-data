# `team_crosswalk`

`team_crosswalk` reshaper -- release tag [`mbb_crosswalk`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mbb_crosswalk).

| | |
|---|---|
| **Builder** | [`R/mbb_11_team_crosswalk_creation.R`](../../R/mbb_11_team_crosswalk_creation.R) |
| **Release tag** | [`mbb_crosswalk`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mbb_crosswalk) |
| **File stem** | `mbb_team_crosswalk_{season}.{parquet,csv,rds}` |
| **Manifested** | no (no load_mbb_*_manifest() loader yet) |
| **Last published** | 2026-08-07 (newest release asset) |
| **Tag created** | 2026-06-13 |
| **Release assets** | 25 |

## Automation

`.github/workflows/daily_mbb.yml` -- daily cron, running `scripts/daily_mbb_data_processor.sh` (the single entrypoint). `-l python` is the default and builds via `mbb_data_build`; `-l R` is the retained rollback path over the R creation scripts.

## Columns

| col_name | type | description |
|---|---|---|
| `season` | Int32 | |
| `espn_team_id` | Int32 | |
| `espn_abbreviation` | String | |
| `espn_display_name` | String | |
| `espn_short_name` | String | |
| `espn_location` | String | |
| `espn_mascot` | String | |
| `espn_conference` | String | |
| `fox_team_id` | String | |
| `fox_team_name` | String | |
| `fox_section` | String | |
| `bart_team` | String | |
| `bart_conf` | String | |
| `kp_team` | String | |
| `kp_conf` | String | |
| `yahoo_team_id` | String | |
| `yahoo_team_name` | String | |
| `fox_match_confidence` | Float64 | |
| `bart_match_confidence` | Float64 | |
| `kp_match_confidence` | Float64 | |
| `match_method` | String | |

## Coverage

| season | rows |
|---:|---:|
| 2026 | 362 |
