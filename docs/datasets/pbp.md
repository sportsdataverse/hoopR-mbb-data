# `pbp`

`pbp` reshaper -- release tag [`espn_mens_college_basketball_pbp`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_pbp).

| | |
|---|---|
| **Builder** | [`python/espn_mbb_01_pbp_creation.py`](../../python/espn_mbb_01_pbp_creation.py) |
| **Release tag** | [`espn_mens_college_basketball_pbp`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_pbp) |
| **File stem** | `play_by_play_{season}.{parquet,csv,rds}` |
| **Manifested** | no (no load_mbb_*_manifest() loader yet) |
| **Last published** | 2026-08-07 (newest release asset) |
| **Tag created** | 2023-03-29 |
| **Release assets** | 70 |

## Automation

`.github/workflows/daily_mbb.yml` -- daily cron, running `scripts/daily_mbb_data_processor.sh` (the single entrypoint). `-l python` is the default and builds via `mbb_data_build`; `-l R` is the retained rollback path over the R creation scripts.

## Columns

| col_name | type | description |
|---|---|---|
| `game_play_number` | Int32 | |
| `id` | Int64 | |
| `sequence_number` | Int32 | |
| `type_id` | Int32 | |
| `type_text` | String | |
| `text` | String | |
| `away_score` | Int32 | |
| `home_score` | Int32 | |
| `period_number` | Int32 | |
| `period_display_value` | String | |
| `clock_display_value` | String | |
| `scoring_play` | Boolean | |
| `score_value` | Int32 | |
| `wallclock` | String | |
| `shooting_play` | Boolean | |
| `coordinate_x_raw` | Float64 | |
| `coordinate_y_raw` | Float64 | |
| `points_attempted` | Int32 | |
| `short_description` | String | |
| `team_id` | Int32 | |
| `athlete_id_1` | Int32 | |
| `athlete_id_2` | Int32 | |
| `game_id` | Int32 | |
| `season` | Int32 | |
| `season_type` | Int32 | |
| `home_team_id` | Int32 | |
| `home_team_name` | String | |
| `home_team_mascot` | String | |
| `home_team_abbrev` | String | |
| `home_team_name_alt` | String | |
| `away_team_id` | Int32 | |
| `away_team_name` | String | |
| `away_team_mascot` | String | |
| `away_team_abbrev` | String | |
| `away_team_name_alt` | String | |
| `game_spread` | Float64 | |
| `home_favorite` | Boolean | |
| `game_spread_available` | Boolean | |
| `home_team_spread` | Float64 | |
| `half` | Int32 | |
| `time` | String | |
| `clock_minutes` | Int32 | |
| `clock_seconds` | Int32 | |
| `home_timeout_called` | Boolean | |
| `away_timeout_called` | Boolean | |
| `lag_period` | Int32 | |
| `lead_period` | Int32 | |
| `lag_half` | Int32 | |
| `lead_half` | Int32 | |
| `start_period_seconds_remaining` | Int32 | |
| `start_game_seconds_remaining` | Int32 | |
| `end_period_seconds_remaining` | Int32 | |
| `end_game_seconds_remaining` | Int32 | |
| `coordinate_x` | Float64 | |
| `coordinate_y` | Float64 | |
| `game_date` | Date | |
| `game_date_time` | Datetime(time_unit='us', time_zone='America/New_York') | |
| `athlete_name_1` | String | |
| `athlete_name_2` | String | |
| `athlete_name_3` | String | |
| `pregame_home_prob` | Float64 | |
| `home_win_prob` | Float64 | |

## Coverage

| season | rows |
|---:|---:|
| 2003 | 5,182 |
| 2006 | 314,119 |
| 2007 | 457,893 |
| 2008 | 798,658 |
| 2009 | 1,141,127 |
| 2010 | 1,271,470 |
| 2011 | 1,131,683 |
| 2012 | 1,118,161 |
| 2013 | 1,257,727 |
| 2014 | 1,670,308 |
| 2015 | 1,737,302 |
| 2016 | 1,803,531 |
| 2017 | 1,818,853 |
| 2018 | 1,827,395 |
| 2019 | 1,787,332 |
| 2020 | 1,746,980 |
| 2021 | 1,296,252 |
| 2022 | 1,860,561 |
| 2023 | 1,957,375 |
| 2024 | 2,004,997 |
| 2025 | 2,190,101 |
| 2026 | 2,915,731 |
