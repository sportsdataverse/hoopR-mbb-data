# hoopR-mbb-data
hoopR Men's College Basketball Data 2003 - Present


```mermaid
  graph LR;
    A[hoopR-mbb-raw]-->B[hoopR-mbb-data];
    B[hoopR-mbb-data]-->C1[espn_mens_college_basketball_pbp];
    B[hoopR-mbb-data]-->C2[espn_mens_college_basketball_team_boxscores];
    B[hoopR-mbb-data]-->C3[espn_mens_college_basketball_player_boxscores];

```

## hoopR ESPN MBB workflow diagram

```mermaid
  graph LR;
    A[hoopR-mbb-raw]-->B[hoopR-mbb-data];
    B[hoopR-mbb-data]-->C1[espn_mens_college_basketball_schedules];
    B[hoopR-mbb-data]-->C2[espn_mens_college_basketball_pbp];
    B[hoopR-mbb-data]-->C3[espn_mens_college_basketball_team_boxscores];
    B[hoopR-mbb-data]-->C4[espn_mens_college_basketball_player_boxscores];
    B[hoopR-mbb-data]-->C5[espn_mens_college_basketball_rosters];
    B[hoopR-mbb-data]-->C6[espn_mens_college_basketball_game_rosters];
    B[hoopR-mbb-data]-->C7[espn_mens_college_basketball_player_core];
    B[hoopR-mbb-data]-->C8[espn_mens_college_basketball_player_season_stats];
    B[hoopR-mbb-data]-->C9[espn_mens_college_basketball_team_season_stats];
    B[hoopR-mbb-data]-->C10[espn_mens_college_basketball_standings];
    B[hoopR-mbb-data]-->C11[espn_mens_college_basketball_officials];
    B[hoopR-mbb-data]-->C12[espn_mens_college_basketball_shots];
    B[hoopR-mbb-data]-->C13[mbb_crosswalk];
```

```mermaid
flowchart TB;
    subgraph A[hoopR-mbb-raw];
        direction TB;
        A0[scripts/daily_mbb_scraper.sh]-->A1[python/espn_mbb_01_schedules_scrape.py];
        A1[python/espn_mbb_01_schedules_scrape.py]-->A2[python/espn_mbb_02_pbp_scrape.py];
        A2[python/espn_mbb_02_pbp_scrape.py]-->A3[python/espn_mbb_03_standings_scrape.py];
        A3[python/espn_mbb_03_standings_scrape.py]-->A4[python/espn_mbb_04_game_rosters_scrape.py];
        A4[python/espn_mbb_04_game_rosters_scrape.py]-->A5[python/espn_mbb_06_player_stats_scrape.py];
        A5[python/espn_mbb_06_player_stats_scrape.py]-->A6[python/espn_mbb_07_team_stats_scrape.py];
        A6[python/espn_mbb_07_team_stats_scrape.py]-->A7[python/espn_mbb_08_team_rosters_scrape.py];
        A7[python/espn_mbb_08_team_rosters_scrape.py]-->A8[python/espn_mbb_09_player_core_scrape.py];
    end;

    subgraph B[hoopR-mbb-data];
        direction TB;
        B0[scripts/daily_mbb_data_processor.sh]-->B1[python/espn_mbb_01_pbp_creation.py];
        B1[python/espn_mbb_01_pbp_creation.py]-->B2[python/espn_mbb_02_team_box_creation.py];
        B2[python/espn_mbb_02_team_box_creation.py]-->B3[python/espn_mbb_03_player_box_creation.py];
        B3[python/espn_mbb_03_player_box_creation.py]-->B4[python/espn_mbb_04_rosters_creation.py];
        B4[python/espn_mbb_04_rosters_creation.py]-->B5[python/espn_mbb_05_player_season_stats_creation.py];
        B5[python/espn_mbb_05_player_season_stats_creation.py]-->B6[python/espn_mbb_06_team_season_stats_creation.py];
        B6[python/espn_mbb_06_team_season_stats_creation.py]-->B7[python/espn_mbb_07_standings_creation.py];
        B7[python/espn_mbb_07_standings_creation.py]-->B8[python/espn_mbb_09_game_rosters_creation.py];
        B8[python/espn_mbb_09_game_rosters_creation.py]-->B9[python/espn_mbb_10_officials_creation.py];
        B9[python/espn_mbb_10_officials_creation.py]-->B10[python/espn_mbb_11_team_crosswalk_creation.py];
        B10[python/espn_mbb_11_team_crosswalk_creation.py]-->B11[python/espn_mbb_12_schedule_crosswalk_creation.py];
        B11[python/espn_mbb_12_schedule_crosswalk_creation.py]-->B12[python/espn_mbb_13_player_crosswalk_creation.py];
        B12[python/espn_mbb_13_player_crosswalk_creation.py]-->B13[python/espn_mbb_14_schedules_creation.py];
        B13[python/espn_mbb_14_schedules_creation.py]-->B14[python/espn_mbb_15_shots_creation.py];
        B14[python/espn_mbb_15_shots_creation.py]-->B15[python/espn_mbb_16_player_core_creation.py];
    end;

    subgraph C[sportsdataverse-data Releases];
        direction TB;
        C1[espn_mens_college_basketball_schedules];
        C2[espn_mens_college_basketball_pbp];
        C3[espn_mens_college_basketball_team_boxscores];
        C4[espn_mens_college_basketball_player_boxscores];
        C5[espn_mens_college_basketball_rosters];
        C6[espn_mens_college_basketball_game_rosters];
        C7[espn_mens_college_basketball_player_core];
        C8[espn_mens_college_basketball_player_season_stats];
        C9[espn_mens_college_basketball_team_season_stats];
        C10[espn_mens_college_basketball_standings];
        C11[espn_mens_college_basketball_officials];
        C12[espn_mens_college_basketball_shots];
        C13[mbb_crosswalk];
    end;

    A-->B;
    B-->C;
```

`scripts/daily_mbb_scraper.sh` and `scripts/daily_mbb_data_processor.sh` are the
daily drivers (the `00` role); stage numbers are intended build order, not run order.

Stage numbers are stable cross-repo identifiers, so holes are expected —
`05` (draft) is NBA-only and intentionally vacant on the MBB side.

[hoopR-mbb-raw repository (source: ESPN)](https://github.com/sportsdataverse/hoopR-mbb-raw)

[hoopR-mbb-data repository (source: ESPN)](https://github.com/sportsdataverse/hoopR-mbb-data)

[hoopR-nba-raw repository (source: ESPN)](https://github.com/sportsdataverse/hoopR-nba-raw)

[hoopR-nba-data repository (source: ESPN)](https://github.com/sportsdataverse/hoopR-nba-data)

[hoopR-nba-stats-raw repository (source: NBA Stats)](https://github.com/sportsdataverse/hoopR-nba-stats-raw)

[hoopR-nba-stats-data repository (source: NBA Stats)](https://github.com/sportsdataverse/hoopR-nba-stats-data)

[ncaa-mbb-hoops-raw repository (source: stats.ncaa.org)](https://github.com/sportsdataverse/ncaa-mbb-hoops-raw)

[ncaa-mbb-hoops-data repository (source: stats.ncaa.org)](https://github.com/sportsdataverse/ncaa-mbb-hoops-data)

[hoopR-kp-data repository (source: KenPom, dormant)](https://github.com/sportsdataverse/hoopR-kp-data)

## Stage numbering

`R/espn_mbb_NN_*.R` stage numbers are stable identifiers aligned across the
sibling hoopR/wehoop data repos, so holes are expected — `08` is intentionally
vacant here (it maps to the NBA-only draft dataset,
`espn_nba_08_draft_creation.R` in hoopR-nba-data). Never renumber existing
stages to close a hole.

## Datasets

<!-- BEGIN GENERATED: datasets -->
| Script | Dataset | Release tag | Last published |
|---|---|---|---|
| [`python/espn_mbb_01_pbp_creation.py`](python/espn_mbb_01_pbp_creation.py) | [`pbp`](docs/datasets/pbp.md) | [`espn_mens_college_basketball_pbp`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_pbp) | 2026-08-12 |
| [`python/espn_mbb_02_team_box_creation.py`](python/espn_mbb_02_team_box_creation.py) | [`team_box`](docs/datasets/team_box.md) | [`espn_mens_college_basketball_team_boxscores`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_team_boxscores) | 2026-08-12 |
| [`python/espn_mbb_03_player_box_creation.py`](python/espn_mbb_03_player_box_creation.py) | [`player_box`](docs/datasets/player_box.md) | [`espn_mens_college_basketball_player_boxscores`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_player_boxscores) | 2026-08-12 |
| [`python/espn_mbb_04_rosters_creation.py`](python/espn_mbb_04_rosters_creation.py) | [`rosters`](docs/datasets/rosters.md) | [`espn_mens_college_basketball_rosters`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_rosters) | 2026-08-12 |
| [`python/espn_mbb_05_player_season_stats_creation.py`](python/espn_mbb_05_player_season_stats_creation.py) | [`player_season_stats`](docs/datasets/player_season_stats.md) | [`espn_mens_college_basketball_player_season_stats`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_player_season_stats) | 2026-08-12 |
| [`python/espn_mbb_06_team_season_stats_creation.py`](python/espn_mbb_06_team_season_stats_creation.py) | [`team_season_stats`](docs/datasets/team_season_stats.md) | [`espn_mens_college_basketball_team_season_stats`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_team_season_stats) | 2026-08-12 |
| [`python/espn_mbb_07_standings_creation.py`](python/espn_mbb_07_standings_creation.py) | [`standings`](docs/datasets/standings.md) | [`espn_mens_college_basketball_standings`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_standings) | 2026-08-12 |
| [`python/espn_mbb_09_game_rosters_creation.py`](python/espn_mbb_09_game_rosters_creation.py) | [`game_rosters`](docs/datasets/game_rosters.md) | [`espn_mens_college_basketball_game_rosters`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_game_rosters) | 2026-08-12 |
| [`python/espn_mbb_10_officials_creation.py`](python/espn_mbb_10_officials_creation.py) | [`officials`](docs/datasets/officials.md) | [`espn_mens_college_basketball_officials`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_officials) | 2026-08-12 |
| [`python/espn_mbb_11_team_crosswalk_creation.py`](python/espn_mbb_11_team_crosswalk_creation.py) | [`team_crosswalk`](docs/datasets/team_crosswalk.md) | [`mbb_crosswalk`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mbb_crosswalk) | 2026-08-12 |
| [`python/espn_mbb_12_schedule_crosswalk_creation.py`](python/espn_mbb_12_schedule_crosswalk_creation.py) | [`schedule_crosswalk`](docs/datasets/schedule_crosswalk.md) | [`mbb_crosswalk`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mbb_crosswalk) | 2026-08-12 |
| [`python/espn_mbb_13_player_crosswalk_creation.py`](python/espn_mbb_13_player_crosswalk_creation.py) | [`player_crosswalk`](docs/datasets/player_crosswalk.md) | [`mbb_crosswalk`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mbb_crosswalk) | 2026-08-12 |
| [`python/espn_mbb_14_schedules_creation.py`](python/espn_mbb_14_schedules_creation.py) | [`schedules`](docs/datasets/schedules.md) | [`espn_mens_college_basketball_schedules`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_schedules) | 2026-08-12 |
| [`python/espn_mbb_15_shots_creation.py`](python/espn_mbb_15_shots_creation.py) | [`shots`](docs/datasets/shots.md) | [`espn_mens_college_basketball_shots`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_shots) | 2026-08-12 |
| [`python/espn_mbb_16_player_core_creation.py`](python/espn_mbb_16_player_core_creation.py) | [`player_core`](docs/datasets/player_core.md) | [`espn_mens_college_basketball_player_core`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_player_core) | 2026-08-12 |
<!-- END GENERATED: datasets -->

## Reports & explainers

<!-- BEGIN GENERATED: reports -->

| Report | What it is | Last updated |
|---|---|---|
| [Dataset docs (column-level, generated)](docs/datasets/) | 15 files, one per item | 2026-08-12 |

<!-- END GENERATED: reports -->

## Automation & status

<!-- BEGIN GENERATED: status -->

| workflow | schedule | last run |
|---|---|---|
| [![daily_mbb.yml](https://github.com/sportsdataverse/hoopR-mbb-data/actions/workflows/daily_mbb.yml/badge.svg)](https://github.com/sportsdataverse/hoopR-mbb-data/actions/workflows/daily_mbb.yml) | days 18-31 07:00 UTC in Oct; daily 07:00 UTC in Nov-Dec; daily 07:00 UTC in Jan-Mar; days 1-30 07:00 UTC in Apr | 2026-08-19 |
| [![mbb_models_cron.yml](https://github.com/sportsdataverse/hoopR-mbb-data/actions/workflows/mbb_models_cron.yml/badge.svg)](https://github.com/sportsdataverse/hoopR-mbb-data/actions/workflows/mbb_models_cron.yml) | daily 13:00 UTC in Nov-Dec; daily 13:00 UTC in Jan-Apr | never run |
| [![orphan_scripts.yml](https://github.com/sportsdataverse/hoopR-mbb-data/actions/workflows/orphan_scripts.yml/badge.svg)](https://github.com/sportsdataverse/hoopR-mbb-data/actions/workflows/orphan_scripts.yml) | on push / PR / dispatch | 2026-08-26 |
| [![tests.yml](https://github.com/sportsdataverse/hoopR-mbb-data/actions/workflows/tests.yml/badge.svg)](https://github.com/sportsdataverse/hoopR-mbb-data/actions/workflows/tests.yml) | on push / PR / dispatch | 2026-08-27 |

| release tag | assets | size | last publish |
|---|---:|---:|---|
| [`espn_mens_college_basketball_schedules`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_schedules) | 88 | 501.5 MB | 2026-08-19 |
| [`espn_mens_college_basketball_pbp`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_pbp) | 70 | 13,482.6 MB | 2026-08-12 |
| [`espn_mens_college_basketball_team_boxscores`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_team_boxscores) | 76 | 142.7 MB | 2026-08-12 |
| [`espn_mens_college_basketball_player_boxscores`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_player_boxscores) | 76 | 1,865.0 MB | 2026-08-26 |
| [`espn_mens_college_basketball_rosters`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_rosters) | 15 | 17.1 MB | 2026-08-19 |
| [`espn_mens_college_basketball_game_rosters`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_game_rosters) | 56 | 883.7 MB | 2026-08-12 |
| [`espn_mens_college_basketball_player_core`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_player_core) | 72 | 82.1 MB | 2026-08-12 |
| [`espn_mens_college_basketball_player_season_stats`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_player_season_stats) | 12 | 176.2 MB | 2026-08-12 |
| [`espn_mens_college_basketball_team_season_stats`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_team_season_stats) | 78 | 138.2 MB | 2026-08-12 |
| [`espn_mens_college_basketball_standings`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_standings) | 78 | 167.9 MB | 2026-08-12 |
| [`espn_mens_college_basketball_officials`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_officials) | 54 | 21.1 MB | 2026-08-12 |
| [`espn_mens_college_basketball_shots`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_shots) | 71 | 1,837.2 MB | 2026-08-12 |
| [`mbb_crosswalk`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mbb_crosswalk) | 25 | 4.0 MB | 2026-08-12 |

<!-- END GENERATED: status -->
