# `player_box`

`player_box` reshaper -- release tag [`espn_mens_college_basketball_player_boxscores`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_player_boxscores).

| | |
|---|---|
| **Builder** | [`python/espn_mbb_03_player_box_creation.py`](../../python/espn_mbb_03_player_box_creation.py) |
| **Release tag** | [`espn_mens_college_basketball_player_boxscores`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_player_boxscores) |
| **File stem** | `player_box_{season}.{parquet,csv,rds}` |
| **Manifested** | no (no load_mbb_*_manifest() loader yet) |
| **Last published** | — (newest release asset) |
| **Tag created** | — |
| **Release assets** | — |

## Automation

`.github/workflows/daily_mbb.yml` -- daily cron, running `scripts/daily_mbb_R_processor.sh` (R, the current daily producer). `scripts/daily_mbb_python_processor.sh` runs the parity-tested Python build in `mbb_data_build` but does not yet drive daily CI.

## Columns

| col_name | type | description |
|---|---|---|
| `game_id` | Int32 | |
| `season` | Int32 | |
| `season_type` | Int32 | |
| `game_date` | Date | |
| `game_date_time` | Datetime(time_unit='us', time_zone='America/New_York') | |
| `athlete_id` | Int32 | |
| `athlete_display_name` | String | |
| `team_id` | Int32 | |
| `team_name` | String | |
| `team_location` | String | |
| `team_short_display_name` | String | |
| `minutes` | Float64 | |
| `field_goals_made` | Int32 | |
| `field_goals_attempted` | Int32 | |
| `three_point_field_goals_made` | Int32 | |
| `three_point_field_goals_attempted` | Int32 | |
| `free_throws_made` | Int32 | |
| `free_throws_attempted` | Int32 | |
| `offensive_rebounds` | Int32 | |
| `defensive_rebounds` | Int32 | |
| `rebounds` | Int32 | |
| `assists` | Int32 | |
| `steals` | Int32 | |
| `blocks` | Int32 | |
| `turnovers` | Int32 | |
| `fouls` | Int32 | |
| `points` | Int32 | |
| `starter` | Boolean | |
| `ejected` | Boolean | |
| `did_not_play` | Boolean | |
| `athlete_jersey` | String | |
| `athlete_short_name` | String | |
| `athlete_headshot_href` | String | |
| `athlete_position_name` | String | |
| `athlete_position_abbreviation` | String | |
| `team_display_name` | String | |
| `team_uid` | String | |
| `team_slug` | String | |
| `team_logo` | String | |
| `team_abbreviation` | String | |
| `team_color` | String | |
| `team_alternate_color` | String | |
| `home_away` | String | |
| `team_winner` | Boolean | |
| `team_score` | Int32 | |
| `opponent_team_id` | Int32 | |
| `opponent_team_name` | String | |
| `opponent_team_location` | String | |
| `opponent_team_display_name` | String | |
| `opponent_team_abbreviation` | String | |
| `opponent_team_logo` | String | |
| `opponent_team_color` | String | |
| `opponent_team_alternate_color` | String | |
| `opponent_team_score` | Int32 | |
| `active` | Boolean | |

## Coverage

| season | rows |
|---:|---:|
| 2003 | 26 |
| 2004 | 289 |
| 2005 | 112,140 |
| 2006 | 126,329 |
| 2007 | 140,099 |
| 2008 | 149,000 |
| 2009 | 145,485 |
| 2010 | 145,881 |
| 2011 | 146,552 |
| 2012 | 148,383 |
| 2013 | 152,419 |
| 2014 | 182,737 |
| 2015 | 182,657 |
| 2016 | 182,524 |
| 2017 | 182,558 |
| 2018 | 186,066 |
| 2019 | 185,819 |
| 2020 | 181,675 |
| 2021 | 134,917 |
| 2022 | 191,555 |
| 2023 | 196,589 |
| 2024 | 198,586 |
| 2025 | 207,623 |
| 2026 | 196,876 |
