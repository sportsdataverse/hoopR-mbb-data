# `shots`

`shots` reshaper -- release tag [`espn_mens_college_basketball_shots`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_shots).

| | |
|---|---|
| **Builder** | [`python/espn_mbb_15_shots_creation.py`](../../python/espn_mbb_15_shots_creation.py) |
| **Release tag** | [`espn_mens_college_basketball_shots`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_shots) |
| **File stem** | `shots_{season}.{parquet,csv,rds}` |
| **Manifested** | yes |
| **Last published** | — (newest release asset) |
| **Tag created** | — |
| **Release assets** | — |

## Automation

`.github/workflows/daily_mbb.yml` -- daily cron, running `scripts/daily_mbb_data_processor.sh` (the single entrypoint). `-l python` is the default and builds via `mbb_data_build`; `-l R` is the retained rollback path over the R creation scripts.

## Columns

| col_name | type | description |
|---|---|---|
| `game_id` | Int32 | |
| `season` | Int32 | |
| `period_number` | Int32 | |
| `clock_display_value` | String | |
| `team_id` | Int32 | |
| `athlete_id_1` | Int32 | |
| `athlete_id_2` | Int32 | |
| `type_id` | Int32 | |
| `type_text` | String | |
| `scoring_play` | Boolean | |
| `score_value` | Int32 | |
| `coordinate_x` | Float64 | |
| `coordinate_y` | Float64 | |
| `coordinate_x_raw` | Float64 | |
| `coordinate_y_raw` | Float64 | |
| `athlete_name_1` | String | |
| `athlete_name_2` | String | |
| `team_name` | String | |
| `team_mascot` | String | |
| `team_abbrev` | String | |

## Coverage

| season | rows |
|---:|---:|
| 2003 | 2,815 |
| 2006 | 165,562 |
| 2007 | 243,478 |
| 2008 | 420,305 |
| 2009 | 552,858 |
| 2010 | 588,378 |
| 2011 | 523,999 |
| 2012 | 515,845 |
| 2013 | 580,497 |
| 2014 | 784,865 |
| 2015 | 803,389 |
| 2016 | 851,494 |
| 2017 | 857,941 |
| 2018 | 860,585 |
| 2019 | 839,915 |
| 2020 | 818,482 |
| 2021 | 608,140 |
| 2022 | 873,219 |
| 2023 | 920,595 |
| 2024 | 957,173 |
| 2025 | 936,510 |
| 2026 | 991,836 |
