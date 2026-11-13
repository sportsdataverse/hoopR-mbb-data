# `schedule_crosswalk`

`schedule_crosswalk` reshaper -- release tag [`mbb_crosswalk`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mbb_crosswalk).

| | |
|---|---|
| **Builder** | [`python/espn_mbb_12_schedule_crosswalk_creation.py`](../../python/espn_mbb_12_schedule_crosswalk_creation.py) |
| **Release tag** | [`mbb_crosswalk`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mbb_crosswalk) |
| **File stem** | `mbb_schedule_crosswalk_{season}.{parquet,csv,rds}` |
| **Manifested** | yes |
| **Last published** | 2026-11-13 (newest release asset) |
| **Tag created** | 2026-06-13 |
| **Release assets** | 172 |

## Load it

| | |
|---|---|
| **R** | `hoopR::load_mbb_schedule_crosswalk()` |
| **Python** | `sportsdataverse.mbb.load_mbb_schedule_crosswalk()` |

## Join keys

| column | joins to |
|---|---|
| `home_espn_team_id` | ESPN team id; the same id in every ESPN dataset here (joins `team_id`). |
| `away_espn_team_id` | ESPN team id; the same id in every ESPN dataset here (joins `team_id`). |
| `espn_game_id` | ESPN game id; joins `game_id`. |
| `kp_game_id` | KenPom id (hoopR-kp-data). |
| `fox_game_id` | Fox Sports id. |
| `yahoo_game_id` | Yahoo Sports id. |

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
| 2008 | 5,630 |
| 2009 | 5,766 |
| 2010 | 5,816 |
| 2011 | 5,820 |
| 2012 | 5,811 |
| 2013 | 5,892 |
| 2014 | 6,021 |
| 2015 | 5,969 |
| 2016 | 5,995 |
| 2017 | 6,009 |
| 2018 | 6,037 |
| 2019 | 6,075 |
| 2020 | 5,781 |
| 2021 | 5,283 |
| 2022 | 6,392 |
| 2023 | 6,267 |
| 2024 | 6,254 |
| 2025 | 6,302 |
| 2026 | 6,319 |
| 2027 | 6,121 |
