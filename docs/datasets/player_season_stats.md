# `player_season_stats`

`player_season_stats` reshaper -- release tag [`espn_mens_college_basketball_player_season_stats`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_player_season_stats).

| | |
|---|---|
| **Builder** | [`python/espn_mbb_05_player_season_stats_creation.py`](../../python/espn_mbb_05_player_season_stats_creation.py) |
| **Release tag** | [`espn_mens_college_basketball_player_season_stats`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_player_season_stats) |
| **File stem** | `player_season_stats_{season}.{parquet,csv,rds}` |
| **Manifested** | yes |
| **Last published** | 2026-11-13 (newest release asset) |
| **Tag created** | 2026-05-30 |
| **Release assets** | 80 |

## Load it

| | |
|---|---|
| **R** | `hoopR::load_mbb_player_stats()` |
| **Python** | `sportsdataverse.mbb.load_mbb_player_season_stats()` |

## Join keys

| column | joins to |
|---|---|
| `athlete_id` | ESPN athlete id. `player_core` holds identity + bio; `player_box`, `rosters`, `player_season_stats` and `game_rosters` key on it. `player_crosswalk.espn_athlete_id` maps it to KenPom / Fox / Yahoo player ids. |
| `team_id` | ESPN team id. Shared by every ESPN dataset here; `team_crosswalk.espn_team_id` maps it to Fox / Yahoo team ids. |

## Automation

`.github/workflows/daily_mbb.yml` -- daily cron, running `scripts/daily_mbb_data_processor.sh` (the single entrypoint). `-l python` is the default and builds via `mbb_data_build`; `-l R` is the retained rollback path over the R creation scripts.

## Columns

| col_name | type | description |
|---|---|---|
| `season` | Int32 | |
| `athlete_id` | Int32 | |
| `athlete_display_name` | String | |
| `athlete_position_abbreviation` | String | |
| `athlete_jersey` | String | |
| `team_id` | Int32 | |
| `team_slug` | String | |
| `team_display_name` | String | |
| `category` | String | |
| `stat_label` | String | |
| `stat_name` | String | |
| `stat_display_name` | String | |
| `stat_description` | String | |
| `display_value` | String | |
| `value` | Float64 | |

## Coverage

| season | rows |
|---:|---:|
| 2003 | 1,122 |
| 2004 | 11,967 |
| 2005 | 197,282 |
| 2006 | 209,534 |
| 2007 | 276,526 |
| 2008 | 287,587 |
| 2009 | 226,847 |
| 2010 | 236,988 |
| 2011 | 233,383 |
| 2012 | 234,935 |
| 2013 | 245,478 |
| 2014 | 306,564 |
| 2015 | 309,021 |
| 2016 | 354,831 |
| 2017 | 325,251 |
| 2018 | 321,843 |
| 2019 | 310,828 |
| 2020 | 366,332 |
| 2021 | 288,683 |
| 2022 | 392,822 |
| 2023 | 409,705 |
| 2024 | 417,121 |
| 2025 | 416,788 |
| 2026 | 426,040 |
| 2027 | 286,656 |
