# `rosters`

`rosters` reshaper -- release tag [`espn_mens_college_basketball_rosters`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_rosters).

| | |
|---|---|
| **Builder** | [`python/espn_mbb_04_rosters_creation.py`](../../python/espn_mbb_04_rosters_creation.py) |
| **Release tag** | [`espn_mens_college_basketball_rosters`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_rosters) |
| **File stem** | `rosters_{season}.{parquet,csv,rds}` |
| **Manifested** | yes |
| **Last published** | — (newest release asset) |
| **Tag created** | — |
| **Release assets** | — |

## Automation

`.github/workflows/daily_mbb.yml` -- daily cron, running `scripts/daily_mbb_R_processor.sh` (R, the current daily producer). `scripts/daily_mbb_python_processor.sh` runs the parity-tested Python build in `mbb_data_build` but does not yet drive daily CI.

## Columns

| col_name | type | description |
|---|---|---|
| `season` | Int32 | |
| `team_id` | Int32 | |
| `team_slug` | String | |
| `team_abbreviation` | String | |
| `team_display_name` | String | |
| `team_short_display_name` | String | |
| `team_color` | String | |
| `team_alternate_color` | String | |
| `team_logo` | String | |
| `athlete_id` | String | |
| `uid` | String | |
| `guid` | String | |
| `full_name` | String | |
| `display_name` | String | |
| `short_name` | String | |
| `first_name` | String | |
| `last_name` | String | |
| `jersey` | String | |
| `position_abbreviation` | String | |
| `position_name` | String | |
| `position_id` | String | |
| `height` | String | |
| `weight` | String | |
| `age` | String | |
| `date_of_birth` | String | |
| `birth_place_city` | String | |
| `birth_place_state` | String | |
| `birth_place_country` | String | |
| `experience_years` | String | |
| `experience_display_value` | String | |
| `headshot_href` | String | |
| `headshot_alt` | String | |
| `link_web` | String | |
| `status_id` | String | |
| `status_name` | String | |
| `status_type` | String | |

## Coverage

| season | rows |
|---:|---:|
| 2025 | 13,056 |
| 2026 | 12,438 |
