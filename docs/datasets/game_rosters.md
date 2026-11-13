# `game_rosters`

`game_rosters` reshaper -- release tag [`espn_mens_college_basketball_game_rosters`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_game_rosters).

| | |
|---|---|
| **Builder** | [`python/espn_mbb_09_game_rosters_creation.py`](../../python/espn_mbb_09_game_rosters_creation.py) |
| **Release tag** | [`espn_mens_college_basketball_game_rosters`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_game_rosters) |
| **File stem** | `game_rosters_{season}.{parquet,csv,rds}` |
| **Manifested** | yes |
| **Last published** | 2026-11-13 (newest release asset) |
| **Tag created** | 2026-05-30 |
| **Release assets** | 83 |

## Load it

| | |
|---|---|
| **R** | `hoopR::load_mbb_game_rosters()` |
| **Python** | `sportsdataverse.mbb.load_mbb_game_rosters()` |

## Join keys

| column | joins to |
|---|---|
| `game_id` | ESPN game id. `schedules` has one row per game; `pbp`, `shots`, `team_box`, `player_box`, `game_rosters` and `officials` key on it. `schedule_crosswalk.espn_game_id` maps it to KenPom / Fox / Yahoo game ids. |
| `team_id` | ESPN team id. Shared by every ESPN dataset here; `team_crosswalk.espn_team_id` maps it to Fox / Yahoo team ids. |
| `athlete_id` | ESPN athlete id. `player_core` holds identity + bio; `player_box`, `rosters`, `player_season_stats` and `game_rosters` key on it. `player_crosswalk.espn_athlete_id` maps it to KenPom / Fox / Yahoo player ids. |

## Automation

`.github/workflows/daily_mbb.yml` -- daily cron, running `scripts/daily_mbb_data_processor.sh` (the single entrypoint). `-l python` is the default and builds via `mbb_data_build`; `-l R` is the retained rollback path over the R creation scripts.

## Columns

| col_name | type | description |
|---|---|---|
| `season` | Int32 | |
| `game_id` | Int32 | |
| `team_id` | Int32 | |
| `team_slug` | String | |
| `team_abbreviation` | String | |
| `team_display_name` | String | |
| `home_away` | String | |
| `athlete_id` | Int32 | |
| `athlete_uid` | String | |
| `athlete_guid` | String | |
| `athlete_display_name` | String | |
| `athlete_short_name` | String | |
| `athlete_first_name` | String | |
| `athlete_last_name` | String | |
| `athlete_jersey` | String | |
| `athlete_position` | String | |
| `athlete_headshot` | String | |
| `starter` | Boolean | |
| `did_not_play` | Boolean | |
| `active` | Boolean | |
| `ejected` | Boolean | |
| `reason` | String | |

## Coverage

| season | rows |
|---:|---:|
| 2002 | 129,723 |
| 2003 | 133,672 |
| 2004 | 130,517 |
| 2005 | 132,121 |
| 2006 | 135,680 |
| 2007 | 147,009 |
| 2008 | 146,196 |
| 2009 | 148,615 |
| 2010 | 152,306 |
| 2011 | 149,596 |
| 2012 | 150,709 |
| 2013 | 153,481 |
| 2014 | 182,746 |
| 2015 | 182,656 |
| 2016 | 182,741 |
| 2017 | 182,688 |
| 2018 | 186,099 |
| 2019 | 185,876 |
| 2020 | 181,675 |
| 2021 | 134,917 |
| 2022 | 191,555 |
| 2023 | 196,589 |
| 2024 | 198,653 |
| 2025 | 207,716 |
| 2026 | 196,876 |
| 2027 | 20,992 |
