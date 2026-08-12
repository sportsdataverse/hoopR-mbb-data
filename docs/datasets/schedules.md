# `schedules`

`schedules` reshaper -- release tag [`espn_mens_college_basketball_schedules`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_schedules).

| | |
|---|---|
| **Builder** | [`python/espn_mbb_14_schedules_creation.py`](../../python/espn_mbb_14_schedules_creation.py) |
| **Release tag** | [`espn_mens_college_basketball_schedules`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_schedules) |
| **File stem** | `mbb_schedule_{season}.{parquet,csv,rds}` |
| **Manifested** | no (no load_mbb_*_manifest() loader yet) |
| **Last published** | 2026-08-12 (newest release asset) |
| **Tag created** | 2023-03-29 |
| **Release assets** | 85 |

## Automation

`.github/workflows/daily_mbb.yml` -- daily cron, running `scripts/daily_mbb_data_processor.sh` (the single entrypoint). `-l python` is the default and builds via `mbb_data_build`; `-l R` is the retained rollback path over the R creation scripts.

## Columns

| col_name | type | description |
|---|---|---|
| `id` | Int32 | |
| `uid` | String | |
| `date` | String | |
| `attendance` | Float64 | |
| `time_valid` | Boolean | |
| `neutral_site` | Boolean | |
| `conference_competition` | Boolean | |
| `play_by_play_available` | Boolean | |
| `recent` | Boolean | |
| `start_date` | String | |
| `broadcast` | String | |
| `highlights` | String | |
| `notes_type` | String | |
| `notes_headline` | String | |
| `broadcast_market` | String | |
| `broadcast_name` | String | |
| `type_id` | Int32 | |
| `type_abbreviation` | String | |
| `venue_id` | Int32 | |
| `venue_full_name` | String | |
| `venue_address_city` | String | |
| `venue_address_state` | String | |
| `venue_indoor` | Boolean | |
| `status_clock` | Float64 | |
| `status_display_clock` | String | |
| `status_period` | Float64 | |
| `status_type_id` | Int32 | |
| `status_type_name` | String | |
| `status_type_state` | String | |
| `status_type_completed` | Boolean | |
| `status_type_description` | String | |
| `status_type_detail` | String | |
| `status_type_short_detail` | String | |
| `format_regulation_periods` | Float64 | |
| `home_id` | Int32 | |
| `home_uid` | String | |
| `home_location` | String | |
| `home_name` | String | |
| `home_abbreviation` | String | |
| `home_display_name` | String | |
| `home_short_display_name` | String | |
| `home_color` | String | |
| `home_alternate_color` | String | |
| `home_is_active` | Boolean | |
| `home_venue_id` | Int32 | |
| `home_logo` | String | |
| `home_conference_id` | Int32 | |
| `home_score` | Int32 | |
| `home_winner` | Boolean | |
| `home_current_rank` | Float64 | |
| `home_linescores` | String | |
| `home_records` | String | |
| `away_id` | Int32 | |
| `away_uid` | String | |
| `away_location` | String | |
| `away_name` | String | |
| `away_abbreviation` | String | |
| `away_display_name` | String | |
| `away_short_display_name` | String | |
| `away_color` | String | |
| `away_alternate_color` | String | |
| `away_is_active` | Boolean | |
| `away_venue_id` | Int32 | |
| `away_logo` | String | |
| `away_conference_id` | Int32 | |
| `away_score` | Int32 | |
| `away_winner` | Boolean | |
| `away_current_rank` | Float64 | |
| `away_linescores` | String | |
| `away_records` | String | |
| `game_id` | Int32 | |
| `season` | Int32 | |
| `season_type` | Int32 | |
| `status_type_alt_detail` | String | |
| `tournament_id` | Int32 | |
| `groups_id` | Int32 | |
| `groups_name` | String | |
| `groups_short_name` | String | |
| `groups_is_conference` | Boolean | |
| `game_json` | Boolean | |
| `game_json_url` | String | |
| `game_date_time` | Datetime(time_unit='us', time_zone='America/New_York') | |
| `game_date` | Date | |
| `PBP` | Boolean | |
| `team_box` | Boolean | |
| `player_box` | Boolean | |

## Coverage

| season | rows |
|---:|---:|
| 2003 | 4,990 |
| 2004 | 4,983 |
| 2005 | 5,048 |
| 2006 | 5,172 |
| 2007 | 5,480 |
| 2008 | 5,590 |
| 2009 | 5,726 |
| 2010 | 5,622 |
| 2011 | 5,771 |
| 2012 | 5,776 |
| 2013 | 5,817 |
| 2014 | 5,948 |
| 2015 | 5,932 |
| 2016 | 5,893 |
| 2017 | 5,960 |
| 2018 | 6,003 |
| 2019 | 6,049 |
| 2020 | 5,767 |
| 2021 | 4,285 |
| 2022 | 5,976 |
| 2023 | 6,261 |
| 2024 | 6,249 |
| 2025 | 6,299 |
| 2026 | 6,318 |
