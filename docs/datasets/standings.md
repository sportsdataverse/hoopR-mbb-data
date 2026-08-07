# `standings`

`standings` reshaper -- release tag [`espn_mens_college_basketball_standings`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_standings).

| | |
|---|---|
| **Builder** | [`python/espn_mbb_07_standings_creation.py`](../../python/espn_mbb_07_standings_creation.py) |
| **Release tag** | [`espn_mens_college_basketball_standings`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_standings) |
| **File stem** | `standings_{season}.{parquet,csv,rds}` |
| **Manifested** | yes |
| **Last published** | — (newest release asset) |
| **Tag created** | — |
| **Release assets** | — |

## Automation

`.github/workflows/daily_mbb.yml` -- daily cron, running `scripts/daily_mbb_data_processor.sh` (the single entrypoint). `-l python` is the default and builds via `mbb_data_build`; `-l R` is the retained rollback path over the R creation scripts.

## Columns

| col_name | type | description |
|---|---|---|
| `season` | Int32 | |
| `group_id` | String | |
| `group_name` | String | |
| `group_abbreviation` | String | |
| `group_short_name` | String | |
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
| `stat_name` | String | |
| `stat_display_name` | String | |
| `stat_short_display_name` | String | |
| `stat_description` | String | |
| `stat_abbreviation` | String | |
| `stat_type` | String | |
| `display_value` | String | |
| `value` | Float64 | |

## Coverage

| season | rows |
|---:|---:|
| 2003 | 27,384 |
| 2004 | 27,384 |
| 2005 | 27,636 |
| 2006 | 28,056 |
| 2007 | 28,224 |
| 2008 | 28,644 |
| 2009 | 29,232 |
| 2010 | 29,148 |
| 2011 | 29,064 |
| 2012 | 28,896 |
| 2013 | 29,148 |
| 2014 | 17,234 |
| 2015 | 29,484 |
| 2016 | 29,484 |
| 2017 | 26,586 |
| 2018 | 28,084 |
| 2019 | 28,378 |
| 2020 | 28,476 |
| 2021 | 27,370 |
| 2022 | 30,072 |
| 2023 | 30,492 |
| 2024 | 30,408 |
| 2025 | 30,576 |
| 2026 | 31,332 |
