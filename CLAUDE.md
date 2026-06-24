# CLAUDE.md — hoopR-mbb-data Development Guide

## Repo Overview

`hoopR-mbb-data` is the R-side parser for ESPN men's college basketball
play-by-play and box-score data. It reads the raw per-game JSON committed
by [`hoopR-mbb-raw`](https://github.com/sportsdataverse/hoopR-mbb-raw),
compiles per-season parquet/RDS frames under `mbb/{pbp,team_box,player_box,schedules}/`,
commits the compiled frames back to this repo, and uploads them as release
assets to [`sportsdataverse/sportsdataverse-data`](https://github.com/sportsdataverse/sportsdataverse-data)
via `piggyback`. Downstream, the `hoopR` R package's
`load_mbb_*()` / `update_mbb_db()` helpers read those release assets.

- **Package name** (DESCRIPTION): `hoopR.mbb` (not exported to CRAN; release-asset producer)
- **License**: CC BY 4.0
- **Coverage**: ESPN men's college basketball, 2003 – present
- **R Requirement**: R >= 4.0.0

## Pipeline Position

```
ESPN APIs --[python scrape]--> hoopR-mbb-raw
                                    | (raw per-game JSON on main)
                                    v
                               hoopR-mbb-data  [HERE]
                                    | (compile + commit mbb/*; piggyback upload)
                                    v
                          sportsdataverse-data (release tags)
                                    | piggyback download
                                    v
                              hoopR R package (load_mbb_*, update_mbb_db)
```

The R creation scripts read the per-season raw payload directly from
`https://raw.githubusercontent.com/sportsdataverse/hoopR-mbb-raw/main/mbb/...`,
so changes to the raw repo's tree must be coordinated against the
`R/espn_mbb_0[1-3]_*.R` scripts here.

## Build & Development Commands

The repo is driven by `scripts/daily_mbb_R_processor.sh`, which iterates a
year range, runs a fixed **array of 12 creation scripts** per season
(`espn_mbb_01`…`10` plus the three crosswalk scripts), commits + pushes,
and finally runs `R/run_summary.R` for the whole range. All seasons are
integer years (end year of the NCAA season — e.g. `2025` means 2024-25).

```sh
# Full daily flow for one or more seasons (CI entry point)
bash scripts/daily_mbb_R_processor.sh -s 2025 -e 2025

# Or call each creation script directly when iterating
Rscript R/espn_mbb_01_pbp_creation.R          -s 2025 -e 2025
Rscript R/espn_mbb_02_team_box_creation.R     -s 2025 -e 2025
Rscript R/espn_mbb_03_player_box_creation.R   -s 2025 -e 2025
# … 04 rosters, 05 player_season_stats, 06 team_season_stats,
#    07 standings, 09 game_rosters, 10 officials,
#    mbb_11/12/13 team/schedule/player crosswalk

# Helpers
Rscript R/run_summary.R -s 2025 -e 2025        # per-run cli + $GITHUB_STEP_SUMMARY report
Rscript R/rebuild_mbb_master_schedule.R        # rebuilds mbb_schedule_master.csv (interactive)
Rscript R/minify_json_folders.R                # one-off raw-JSON minifier
Rscript R/0000_create_hoopR_releases_init.R    # one-off: create release tags
Rscript R/0001_push_existing_release_data.R    # one-off: backfill release assets
```

`scripts/daily_mbb_R_processor.sh` only accepts `-s` and `-e` (no `-r`).
Each `Rscript` runs under `||` so one failing script doesn't abort the
season; the worst exit code is captured (`RSCRIPT_RC`) and re-surfaced as a
workflow `::error::` after all requested seasons finish. Whatever datasets
succeeded are committed regardless (per-dataset `tryCatch` keeps partial
output usable).

## Repo Layout

```
R/
  espn_mbb_01_pbp_creation.R          # Reads raw JSON -> mbb/pbp/{rds,parquet}/play_by_play_{year}.{ext}
                                       # + mbb/schedules/{rds,parquet}/mbb_schedule_{year}.{ext}
                                       # Uploads to release tag: espn_mens_college_basketball_pbp
  espn_mbb_02_team_box_creation.R     # -> mbb/team_box/{rds,parquet}/team_box_{year}.{ext}
                                       # Uploads to: espn_mens_college_basketball_team_boxscores
  espn_mbb_03_player_box_creation.R   # -> mbb/player_box/{rds,parquet}/player_box_{year}.{ext}
                                       # Uploads to: espn_mens_college_basketball_player_boxscores
                                       # Also uploads schedules to: espn_mens_college_basketball_schedules
  espn_mbb_04…10_*.R                  # rosters, player/team season stats, standings, game_rosters, officials
  mbb_11_team_crosswalk_creation.R    # Cross-source team crosswalk
  mbb_12_schedule_crosswalk_creation.R# Cross-source schedule crosswalk
  mbb_13_player_crosswalk_creation.R  # Cross-source player crosswalk
  run_summary.R                       # Post-run cli summary -> Action log + $GITHUB_STEP_SUMMARY
  manifest_upload_helper.R            # Shared release-upload helper sourced by creation scripts
  rebuild_mbb_master_schedule.R       # Aggregates all per-season schedules into mbb_schedule_master.csv
  minify_json_folders.R               # JSON whitespace minifier (one-off cleanup)
  0000_create_hoopR_releases_init.R   # One-off: create all release tags on sportsdataverse-data
  0001_push_existing_release_data.R   # One-off: backfill release assets after init

scripts/
  daily_mbb_R_processor.sh            # CI entry point — per-year loop over 12 scripts + commit/push

mbb/
  pbp/{rds,parquet}/                  # play_by_play_{year}.{rds,parquet}
  team_box/{rds,parquet}/             # team_box_{year}.{rds,parquet}
  player_box/{rds,parquet}/           # player_box_{year}.{rds,parquet}
  schedules/{rds,parquet}/            # mbb_schedule_{year}.{rds,parquet}
  {rosters,player_season_stats,team_season_stats,standings,game_rosters,officials,crosswalk}/
logs/                                 # Per-season run logs (committed)
.github/workflows/
  daily_mbb.yml                       # Daily cron + repository_dispatch + workflow_dispatch
```

`mbb/mbb_games_in_data_repo.csv` and `mbb_schedule_master.csv` (at the repo
root) are rebuilt by `rebuild_mbb_master_schedule.R`.

## Release Tags

Each `espn_mbb_*` creation script `piggyback::pb_upload()`s to a fixed
release tag on `sportsdataverse/sportsdataverse-data` (the crosswalk
scripts 11/12/13 publish their own crosswalk tags):

| Script | Release tag | Asset shape |
|--------|-------------|-------------|
| `espn_mbb_01_pbp_creation.R` | `espn_mens_college_basketball_pbp` | `play_by_play_{year}.rds`, `play_by_play_{year}.parquet` |
| `espn_mbb_02_team_box_creation.R` | `espn_mens_college_basketball_team_boxscores` | `team_box_{year}.rds`, `team_box_{year}.parquet` |
| `espn_mbb_03_player_box_creation.R` | `espn_mens_college_basketball_player_boxscores` | `player_box_{year}.rds`, `player_box_{year}.parquet` |
| `espn_mbb_03_player_box_creation.R` (also) | `espn_mens_college_basketball_schedules` | `mbb_schedule_{year}.rds`, `mbb_schedule_{year}.parquet` |
| `espn_mbb_01_pbp_creation.R` (shots subset) | `espn_mens_college_basketball_shots` | `play_by_play_{year}.*` filtered to `shooting_play` |
| `espn_mbb_04_rosters_creation.R` | `espn_mens_college_basketball_rosters` | `rosters_{year}.*` |
| `espn_mbb_05_player_season_stats_creation.R` | `espn_mens_college_basketball_player_season_stats` | `player_season_stats_{year}.*` (long format) |
| `espn_mbb_06_team_season_stats_creation.R` | `espn_mens_college_basketball_team_season_stats` | `team_season_stats_{year}.*` (long format) |
| `espn_mbb_07_standings_creation.R` | `espn_mens_college_basketball_standings` | `standings_{year}.*` |
| `espn_mbb_09_game_rosters_creation.R` | `espn_mens_college_basketball_game_rosters` | `game_rosters_{year}.*` |
| `espn_mbb_10_officials_creation.R` | `espn_mens_college_basketball_officials` | `officials_{year}.*` |

Loaders: `hoopR::load_mbb_{rosters,player_stats,team_stats,standings,game_rosters,officials}()`.
Ported 1:1 from the `hoopR-nba-data` espn_nba_04…10 scripts
(league = `mens-college-basketball`); season stats slice ESPN's
`categories[].statistics[]`; `officials` reuses the `game_rosters`
summary JSON's `gameInfo.officials`. There is no MBB draft (college).

Renaming a release tag here means renaming the corresponding
`load_mbb_*()` URL in the downstream `hoopR` package — the tag strings are
load-bearing.

## Daily Workflow

`.github/workflows/daily_mbb.yml` is the in-repo cron entry point.

- **Triggers**: `schedule` (four `0 7 UTC` crons gated to the MBB season:
  `0 7 18-31 10 *` Oct 18-31, `0 7 * 11-12 *` Nov-Dec, `0 7 * 1-3 *`
  Jan-Mar, `0 7 1-30 4 *` all of April), `repository_dispatch` event-type
  `daily_mbb_data` (fired from `hoopR-mbb-raw`'s push trigger), and
  `workflow_dispatch` with optional `start_year` / `end_year` inputs.
- **Runner**: `windows-latest` with `r-lib/actions/setup-r@v2` (release
  R). The Windows-pinned runner is intentional — historical pipeline
  ran against the maintainer's Windows R library layout.
- **Year resolution**: client_payload commit message (when dispatched
  from raw) carries the year range embedded as digits; fallback uses
  `hoopR::most_recent_mbb_season()`.
- **Entry command**: `bash scripts/daily_mbb_R_processor.sh -s $START -e $END`.
- **Secrets used**:
  - `SDV_GH_TOKEN` for cross-repo `piggyback::pb_upload()` to
    `sportsdataverse-data`.
  - `GITHUB_TOKEN` for in-repo commit/push.

The script's per-year commit pattern (`MBB Data Update (Start: $i End: $i)`)
is load-bearing — the raw repo's `daily_mbb_data` dispatch grep-extracts
the start/end year digits from the commit message.

## Conventions

- **R style**: tidyverse (`snake_case`, 2-space indent, `%>%` pipe).
  The creation scripts use a procedural top-level style — `purrr::map_dfr`
  + `furrr::future_map_dfr` for per-game compile, `data.table` for I/O
  speed, `arrow::write_parquet`/`saveRDS` for output.
- **Parallelism**: `future::plan("multisession")` before the
  `furrr::future_map_dfr` calls; the CI runner has plenty of headroom.
- **HTTP**: This repo does NOT scrape ESPN — it reads `raw.githubusercontent.com`
  URLs of `hoopR-mbb-raw`. Any ESPN parsing bug belongs in the raw repo's
  Python scrapers (or upstream in `sportsdataverse-py`), not here.
- **`hoopR` dependency**: the creation scripts call internal helpers like
  `hoopR:::most_recent_mbb_season()`, `hoopR:::rds_from_url()`, and
  `hoopR:::espn_mbb_pbp()`. If those internals change in `hoopR`,
  re-pin or update the call sites here.
- **`piggyback` dependency**: pinned to `ropensci/piggyback` (DESCRIPTION
  `Remotes:`). `pb_upload(tag=, repo="sportsdataverse/sportsdataverse-data")`
  needs `SDV_GH_TOKEN` with `repo` scope on that org.
- **No package build expected**: although DESCRIPTION declares
  `Package: hoopR.mbb`, this repo is not installed as an R package
  anywhere — it's a release-asset producer. The DESCRIPTION exists so
  `devtools::install_deps()` can resolve `Imports:` for the CI runner.

## Project-Specific Gotchas

- The Windows runner means filesystem case sensitivity and path
  separators are best avoided in new scripts. Stick to `file.path()`
  and lowercase paths.
- The downstream consumer is `hoopR::load_mbb_*()`. If a column rename
  reaches the parquet/RDS shapes here, the corresponding `hoopR`
  loader's column expectations need to be updated in lockstep.
- `R/rebuild_mbb_master_schedule.R` has `.libPaths()` hardcoded to the
  maintainer's Windows path at the top of the file. Don't run it in CI;
  it's an interactive helper.
- The 2003-2008 ESPN payloads are sparser than the modern era — the
  pbp compile silently empties the per-game frame when ESPN's response
  has no actions. Don't refactor that path to error on empty.

## Cross-Repo References

- Upstream raw scraper: <https://github.com/sportsdataverse/hoopR-mbb-raw>
- Release-asset host: <https://github.com/sportsdataverse/sportsdataverse-data>
- Downstream R package: <https://github.com/sportsdataverse/hoopR>
- Sister repos (same shape, different sport/league):
  - <https://github.com/sportsdataverse/hoopR-nba-data>
  - <https://github.com/sportsdataverse/wehoop-wnba-data>
  - <https://github.com/sportsdataverse/wehoop-wbb-data>

## Commit Convention

Use [Conventional Commits](https://www.conventionalcommits.org/) for
human-authored development work:

```
feat(pbp): emit shot_distance column in play_by_play_{year}.parquet
fix(team_box): coerce empty home.id to NA_integer_ in 2004 frames
chore(deps): bump hoopR pin in DESCRIPTION
ci: align daily_mbb.yml cron with hoopR-mbb-raw schedule
```

The daily data-update commits made by `scripts/daily_mbb_R_processor.sh`
follow a load-bearing format that downstream automation parses:

```
MBB Data Update (Start: 2025 End: 2025)
```

Do not change this format casually — the downstream `daily_mbb_data`
dispatch payload extracts the start/end years from the commit message
via `grep -o -E '[0-9]+' | head/tail -1`.

**Important: Never include AI agents or assistants (e.g., Claude, Copilot, Cursor, GPT, Gemini) as co-authors on commits.** Omit all `Co-Authored-By` trailers referencing AI tools. This applies whether the change was generated, refactored, or reviewed with AI assistance — the human author is the sole attributable contributor.
