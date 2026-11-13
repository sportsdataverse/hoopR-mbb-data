# `team_crosswalk`

`team_crosswalk` reshaper -- release tag [`mbb_crosswalk`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mbb_crosswalk).

| | |
|---|---|
| **Builder** | [`python/espn_mbb_11_team_crosswalk_creation.py`](../../python/espn_mbb_11_team_crosswalk_creation.py) |
| **Release tag** | [`mbb_crosswalk`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mbb_crosswalk) |
| **File stem** | `mbb_team_crosswalk_{season}.{parquet,csv,rds}` |
| **Manifested** | yes |
| **Last published** | 2026-11-13 (newest release asset) |
| **Tag created** | 2026-06-13 |
| **Release assets** | 172 |

## Load it

| | |
|---|---|
| **R** | `hoopR::load_mbb_team_crosswalk()` |
| **Python** | `sportsdataverse.mbb.load_mbb_team_crosswalk()` |

## Join keys

| column | joins to |
|---|---|
| `espn_team_id` | ESPN team id; the same id in every ESPN dataset here (joins `team_id`). |
| `fox_team_id` | Fox Sports id. |
| `yahoo_team_id` | Yahoo Sports id. |

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
| `ncaa_team` | String | |
| `espn_pbp_name` | String | |
| `warren_nolan_team` | String | |
| `team_247` | String | |
| `sref_slug` | String | |
| `sref_team` | String | |
| `yahoo_team_id` | String | |
| `yahoo_team_name` | String | |
| `fox_match_confidence` | Float64 | |
| `bart_match_confidence` | Float64 | |
| `kp_match_confidence` | Float64 | |
| `match_method` | String | |

## Coverage

| season | rows |
|---:|---:|
| 2002 | 362 |
| 2014 | 362 |
| 2015 | 362 |
| 2016 | 362 |
| 2017 | 362 |
| 2018 | 362 |
| 2019 | 362 |
| 2020 | 362 |
| 2021 | 362 |
| 2022 | 362 |
| 2023 | 362 |
| 2024 | 362 |
| 2025 | 362 |
| 2026 | 362 |
| 2027 | 365 |
