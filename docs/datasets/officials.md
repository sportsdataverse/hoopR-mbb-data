# `officials`

`officials` reshaper -- release tag [`espn_mens_college_basketball_officials`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_officials).

| | |
|---|---|
| **Builder** | [`python/espn_mbb_10_officials_creation.py`](../../python/espn_mbb_10_officials_creation.py) |
| **Release tag** | [`espn_mens_college_basketball_officials`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_officials) |
| **File stem** | `officials_{season}.{parquet,csv,rds}` |
| **Manifested** | yes |
| **Last published** | 2026-11-13 (newest release asset) |
| **Tag created** | 2026-05-30 |
| **Release assets** | 77 |

## Load it

| | |
|---|---|
| **R** | `hoopR::load_mbb_officials()` |
| **Python** | `sportsdataverse.mbb.load_mbb_officials()` |

## Join keys

| column | joins to |
|---|---|
| `game_id` | ESPN game id. `schedules` has one row per game; `pbp`, `shots`, `team_box`, `player_box`, `game_rosters` and `officials` key on it. `schedule_crosswalk.espn_game_id` maps it to KenPom / Fox / Yahoo game ids. |

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
| 2003 | 13,943 |
| 2004 | 14,195 |
| 2005 | 14,110 |
| 2006 | 14,371 |
| 2007 | 15,399 |
| 2008 | 14,982 |
| 2009 | 1,243 |
| 2011 | 16,287 |
| 2012 | 16,236 |
| 2013 | 16,250 |
| 2014 | 17,609 |
| 2015 | 17,594 |
| 2016 | 17,461 |
| 2017 | 17,541 |
| 2018 | 17,791 |
| 2019 | 17,470 |
| 2020 | 16,761 |
| 2021 | 12,442 |
| 2022 | 17,002 |
| 2023 | 17,980 |
| 2024 | 18,185 |
| 2025 | 18,284 |
| 2026 | 18,483 |
| 2027 | 1,908 |
