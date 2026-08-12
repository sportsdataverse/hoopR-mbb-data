# `team_box`

`team_box` reshaper -- release tag [`espn_mens_college_basketball_team_boxscores`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_team_boxscores).

| | |
|---|---|
| **Builder** | [`python/espn_mbb_02_team_box_creation.py`](../../python/espn_mbb_02_team_box_creation.py) |
| **Release tag** | [`espn_mens_college_basketball_team_boxscores`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_team_boxscores) |
| **File stem** | `team_box_{season}.{parquet,csv,rds}` |
| **Manifested** | no (no load_mbb_*_manifest() loader yet) |
| **Last published** | 2026-08-12 (newest release asset) |
| **Tag created** | 2023-03-29 |
| **Release assets** | 76 |

## Automation

`.github/workflows/daily_mbb.yml` -- daily cron, running `scripts/daily_mbb_data_processor.sh` (the single entrypoint). `-l python` is the default and builds via `mbb_data_build`; `-l R` is the retained rollback path over the R creation scripts.

## Columns

| col_name | type | description |
|---|---|---|
| `game_id` | Int32 | |
| `season` | Int32 | |
| `season_type` | Int32 | |
| `game_date` | Date | |
| `game_date_time` | Datetime(time_unit='us', time_zone='America/New_York') | |
| `team_id` | Int32 | |
| `team_uid` | String | |
| `team_slug` | String | |
| `team_location` | String | |
| `team_name` | String | |
| `team_abbreviation` | String | |
| `team_display_name` | String | |
| `team_short_display_name` | String | |
| `team_color` | String | |
| `team_alternate_color` | String | |
| `team_logo` | String | |
| `team_home_away` | String | |
| `team_score` | Int32 | |
| `team_winner` | Boolean | |
| `assists` | Int32 | |
| `blocks` | Int32 | |
| `defensive_rebounds` | Int32 | |
| `fast_break_points` | String | |
| `field_goal_pct` | Float64 | |
| `field_goals_made` | Int32 | |
| `field_goals_attempted` | Int32 | |
| `flagrant_fouls` | Int32 | |
| `fouls` | Int32 | |
| `free_throw_pct` | Float64 | |
| `free_throws_made` | Int32 | |
| `free_throws_attempted` | Int32 | |
| `largest_lead` | String | |
| `offensive_rebounds` | Int32 | |
| `points_in_paint` | String | |
| `steals` | Int32 | |
| `team_turnovers` | Int32 | |
| `technical_fouls` | Int32 | |
| `three_point_field_goal_pct` | Float64 | |
| `three_point_field_goals_made` | Int32 | |
| `three_point_field_goals_attempted` | Int32 | |
| `total_rebounds` | Int32 | |
| `total_technical_fouls` | Int32 | |
| `total_turnovers` | Int32 | |
| `turnover_points` | String | |
| `turnovers` | Int32 | |
| `opponent_team_id` | Int32 | |
| `opponent_team_uid` | String | |
| `opponent_team_slug` | String | |
| `opponent_team_location` | String | |
| `opponent_team_name` | String | |
| `opponent_team_abbreviation` | String | |
| `opponent_team_display_name` | String | |
| `opponent_team_short_display_name` | String | |
| `opponent_team_color` | String | |
| `opponent_team_alternate_color` | String | |
| `opponent_team_logo` | String | |
| `opponent_team_score` | Int32 | |
| `lead_changes` | String | |
| `lead_percentage` | String | |

## Coverage

| season | rows |
|---:|---:|
| 2003 | 2 |
| 2004 | 24 |
| 2005 | 8,732 |
| 2006 | 9,846 |
| 2007 | 10,488 |
| 2008 | 11,094 |
| 2009 | 11,278 |
| 2010 | 11,218 |
| 2011 | 11,508 |
| 2012 | 11,518 |
| 2013 | 11,598 |
| 2014 | 11,850 |
| 2015 | 11,854 |
| 2016 | 11,762 |
| 2017 | 11,844 |
| 2018 | 12,004 |
| 2019 | 12,094 |
| 2020 | 11,514 |
| 2021 | 8,566 |
| 2022 | 11,930 |
| 2023 | 12,440 |
| 2024 | 12,480 |
| 2025 | 12,572 |
| 2026 | 12,598 |
