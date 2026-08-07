# `player_season_stats`

`player_season_stats` reshaper -- release tag [`espn_mens_college_basketball_player_season_stats`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_player_season_stats).

| | |
|---|---|
| **Builder** | [`python/espn_mbb_05_player_season_stats_creation.py`](../../python/espn_mbb_05_player_season_stats_creation.py) |
| **Release tag** | [`espn_mens_college_basketball_player_season_stats`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_player_season_stats) |
| **File stem** | `player_season_stats_{season}.{parquet,csv,rds}` |
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
| 2025 | 416,788 |
| 2026 | 426,040 |
