# `team_season_stats`

`team_season_stats` reshaper -- release tag [`espn_mens_college_basketball_team_season_stats`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_team_season_stats).

| | |
|---|---|
| **Builder** | [`python/espn_mbb_06_team_season_stats_creation.py`](../../python/espn_mbb_06_team_season_stats_creation.py) |
| **Release tag** | [`espn_mens_college_basketball_team_season_stats`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_team_season_stats) |
| **File stem** | `team_season_stats_{season}.{parquet,csv,rds}` |
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
| 2003 | 23,400 |
| 2004 | 24,390 |
| 2005 | 24,300 |
| 2006 | 24,660 |
| 2007 | 25,290 |
| 2008 | 24,570 |
| 2009 | 24,885 |
| 2010 | 27,495 |
| 2011 | 27,270 |
| 2012 | 27,180 |
| 2013 | 26,280 |
| 2014 | 27,495 |
| 2015 | 27,810 |
| 2016 | 28,800 |
| 2017 | 27,360 |
| 2018 | 27,810 |
| 2019 | 26,865 |
| 2020 | 29,880 |
| 2021 | 22,185 |
| 2022 | 30,555 |
| 2023 | 31,770 |
| 2024 | 32,265 |
| 2025 | 31,500 |
| 2026 | 32,715 |
