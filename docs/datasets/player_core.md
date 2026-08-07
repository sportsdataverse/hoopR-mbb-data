# `player_core`

`player_core` reshaper -- release tag [`espn_mens_college_basketball_player_core`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_player_core).

| | |
|---|---|
| **Builder** | [`python/espn_mbb_16_player_core_creation.py`](../../python/espn_mbb_16_player_core_creation.py) |
| **Release tag** | [`espn_mens_college_basketball_player_core`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_player_core) |
| **File stem** | `player_core_{season}.{parquet,csv,rds}` |
| **Manifested** | no (no load_mbb_*_manifest() loader yet) |
| **Last published** | — (newest release asset) |
| **Tag created** | — |
| **Release assets** | — |

## Automation

`.github/workflows/daily_mbb.yml` -- daily cron, running `scripts/daily_mbb_data_processor.sh` (the single entrypoint). `-l python` is the default and builds via `mbb_data_build`; `-l R` is the retained rollback path over the R creation scripts.

## Columns

| col_name | type | description |
|---|---|---|
| `season` | Int32 | |
| `athlete_id` | Int64 | |
| `guid` | String | |
| `uid` | String | |
| `slug` | String | |
| `type` | String | |
| `first_name` | String | |
| `last_name` | String | |
| `full_name` | String | |
| `display_name` | String | |
| `short_name` | String | |
| `height` | Float64 | |
| `display_height` | String | |
| `weight` | Float64 | |
| `display_weight` | String | |
| `age` | Int32 | |
| `date_of_birth` | String | |
| `birth_city` | String | |
| `birth_state` | String | |
| `birth_country` | String | |
| `jersey` | String | |
| `position_id` | Int32 | |
| `position_name` | String | |
| `position_abbreviation` | String | |
| `position_display_name` | String | |
| `college_id` | Int32 | |
| `current_team_id` | Int32 | |
| `headshot_href` | String | |
| `experience_years` | Int32 | |
| `status_id` | Int32 | |
| `status_name` | String | |
| `status_type` | String | |
| `draft_year` | Int32 | |
| `draft_round` | Int32 | |
| `draft_selection` | Int32 | |
| `active` | Boolean | |

## Coverage

| season | rows |
|---:|---:|
| 2003 | 26 |
| 2004 | 275 |
| 2005 | 4,602 |
| 2006 | 4,945 |
| 2007 | 6,531 |
| 2008 | 6,802 |
| 2009 | 5,296 |
| 2010 | 5,541 |
| 2011 | 5,440 |
| 2012 | 5,470 |
| 2013 | 5,740 |
| 2014 | 10,587 |
| 2015 | 10,844 |
| 2016 | 10,915 |
| 2017 | 10,795 |
| 2018 | 11,340 |
| 2019 | 10,788 |
| 2020 | 12,991 |
| 2021 | 8,670 |
| 2022 | 12,090 |
| 2023 | 12,529 |
| 2024 | 12,548 |
| 2025 | 15,233 |
| 2026 | 12,481 |
