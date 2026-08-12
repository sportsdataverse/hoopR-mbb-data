#!/bin/bash
# Compile hoopR-mbb-data datasets, per season (Python-first cutover).
#
# The 12 raw-derived datasets are built by `mbb_data_build` (parity-validated
# port of espn_mbb_01..10). Build order matters: shots project the built pbp
# parquet; schedules stamp flags from the built pbp/team_box/player_box
# parquets; player_season_stats reads the built player_box for identity.
# Of the crosswalks (mbb_11-13), 12 (schedule) + 13 (player) build in Python;
# 11 (team) stays on R in both modes -- KenPom is a paid feed (see
# R_CROSSWALKS_IN_PY_MODE below). `.rds` is written
# natively by io.write_dataset in the same pass as the parquet, so there is no
# separate serialize step.
#
# Usage: bash scripts/daily_mbb_data_processor.sh -s 2026 -e 2026 [-l python|R]
set -uo pipefail

# -l selects the language for the raw-derived datasets. Python is the
# production default; `-l R` is the rollback path that used to live in a
# separate daily_mbb_R_processor.sh. One script, so the two paths cannot drift
# in season handling, logging or the load-bearing commit format.
while getopts s:e:l: flag; do
  case "${flag}" in
    s) START_YEAR=${OPTARG};;
    e) END_YEAR=${OPTARG};;
    l) LANG_MODE=${OPTARG};;
    *) echo "usage: $0 -s <start> [-e <end>] [-l python|R]" >&2; exit 2;;
  esac
done
START_YEAR=${START_YEAR:-}
END_YEAR=${END_YEAR:-$START_YEAR}
LANG_MODE=${LANG_MODE:-python}
if [ -z "$START_YEAR" ]; then
  echo "usage: $0 -s <start_year> [-e <end_year>] [-l python|R]" >&2
  exit 1
fi
case "$LANG_MODE" in
  python|R) ;;
  *) echo "::error ::unknown -l '$LANG_MODE' (expected python or R)" >&2; exit 2;;
esac

# CI has no local hoopR-mbb-raw checkout -- read raw over HTTP (the reason the
# python builders are dual-mode Path|str). Override with HOOPR_MBB_RAW_ROOT.
RAW_ROOT="${HOOPR_MBB_RAW_ROOT:-https://raw.githubusercontent.com/sportsdataverse/hoopR-mbb-raw/main}"

# Scrape-log conventions: unbuffered + utf-8 so mbb_data_build's timestamped
# log lines land in the Actions console AND the tee'd season logfile live.
export PYTHONUNBUFFERED=1
export PYTHONIOENCODING=utf-8

# Dependency order: pbp/team_box/player_box first (schedules reads their
# game-id sets; shots read the pbp parquet), then the rest.
PY_DATASETS=(
    pbp
    team_box
    player_box
    player_core
    schedules
    shots
    rosters
    player_season_stats
    team_season_stats
    standings
    game_rosters
    officials
)
# The `-l R` rollback path. R has no counterpart for player_core, schedules or
# shots (espn_mbb_01 writes the schedules + shots subsets inline) -- hence the
# gaps, which are deliberate.
R_DATASETS=(
    R/espn_mbb_01_pbp_creation.R
    R/espn_mbb_02_team_box_creation.R
    R/espn_mbb_03_player_box_creation.R
    R/espn_mbb_04_rosters_creation.R
    R/espn_mbb_05_player_season_stats_creation.R
    R/espn_mbb_06_team_season_stats_creation.R
    R/espn_mbb_07_standings_creation.R
    R/espn_mbb_09_game_rosters_creation.R
    R/espn_mbb_10_officials_creation.R
)
# Crosswalks (stages 11-13). PARTIAL flip: schedule_crosswalk (12) and
# player_crosswalk (13) build in Python. 11 (team) joins KenPom, a PAID feed
# sdv-py cannot reach, so a Python build would publish an asset missing its
# kp_* columns -- it keeps running its .R original in BOTH language modes,
# permanently.
#
# `-l R` is the D20 rollback path and runs all three .R scripts unchanged.
R_CROSSWALKS=(
    R/mbb_11_team_crosswalk_creation.R
    R/mbb_12_schedule_crosswalk_creation.R
    R/mbb_13_player_crosswalk_creation.R
)
# ... and in python mode, the one that did NOT flip.
R_CROSSWALKS_IN_PY_MODE=(
    R/mbb_11_team_crosswalk_creation.R
)
PY_CROSSWALKS=(
    schedule_crosswalk
    player_crosswalk
)

mkdir -p logs
ANY_FAILED=0
for i in $(seq "${START_YEAR}" "${END_YEAR}")
do
    LOGFILE="logs/hoopR_mbb_data_logfile_${i}.log"
    TMPLOG=$(mktemp "/tmp/hoopR_mbb_data_logfile_${i}.XXXXXX.log")
    echo "=== Processing MBB data ($LANG_MODE) for season $i ==="
    # Tee inside the block writes to /tmp (untracked) so the `git pull` calls
    # don't trip over their own log output being written to a tracked file.
    # The block records the worst exit code (RSCRIPT_RC) so a failed compile
    # is surfaced to the workflow rather than masked by a successful git push.
    {
        git pull >> /dev/null
        git config --local user.email "action@github.com"
        git config --local user.name "Github Action"
        SEASON_RC=0

        # ::group:: markers collapse each dataset in the Actions UI; in the
        # tee'd season logfile they read as plain section headers.
        run_py() {
            local ds="$1"
            echo "::group::mbb_data_build $ds $i"
            # Run inside python/ so the flat mbb_data_build package is importable
            # (it is not pip-installed; found via CWD/pythonpath). --base ../mbb
            # writes into the repo-root mbb/ tree.
            ( cd python && uv run python -m mbb_data_build \
                --dataset "$ds" -s "$i" -e "$i" --base ../mbb --raw-root "$RAW_ROOT" --publish ) || {
                rc=$?
                echo "::warning ::mbb_data_build $ds for season $i exited with code $rc"
                SEASON_RC=$rc
            }
            echo "::endgroup::"
        }
        run_r() {
            local script="$1"
            echo "::group::$script $i"
            Rscript "$script" -s "$i" -e "$i" || {
                rc=$?
                echo "::warning ::$script for season $i exited with code $rc"
                SEASON_RC=$rc
            }
            echo "::endgroup::"
        }

        # Crosswalks build from LIVE ESPN+Torvik+Fox(+KenPom) sources and are
        # known-fragile (segfaults/timeouts on external flakiness). Best-effort
        # in BOTH languages: a crosswalk failure warns but does NOT fail the run
        # -- the core datasets are the daily deliverable and publish
        # independently above. Hence these do NOT reuse run_r/run_py, which set
        # SEASON_RC.
        run_r_crosswalk() {
            local script="$1"
            echo "::group::$script $i"
            Rscript "$script" -s "$i" -e "$i" || echo "::warning ::$script for season $i exited with code $? (crosswalk; non-fatal, live external source)"
            echo "::endgroup::"
        }
        run_py_crosswalk() {
            local ds="$1"
            echo "::group::mbb_data_build $ds $i"
            ( cd python && uv run python -m mbb_data_build \
                --dataset "$ds" -s "$i" -e "$i" --base ../mbb --raw-root "$RAW_ROOT" --publish ) \
                || echo "::warning ::mbb_data_build $ds for season $i exited with code $? (crosswalk; non-fatal, live external source)"
            echo "::endgroup::"
        }

        if [ "$LANG_MODE" = "R" ]; then
            for SCRIPT in "${R_DATASETS[@]}"; do run_r "$SCRIPT"; done
            for SCRIPT in "${R_CROSSWALKS[@]}"; do run_r_crosswalk "$SCRIPT"; done
        else
            for DS in "${PY_DATASETS[@]}"; do run_py "$DS"; done
            for SCRIPT in "${R_CROSSWALKS_IN_PY_MODE[@]}"; do run_r_crosswalk "$SCRIPT"; done
            for DS in "${PY_CROSSWALKS[@]}"; do run_py_crosswalk "$DS"; done
        fi

        # Win-probability enrichment: republishes play_by_play_$i.parquet with
        # pregame_home_prob + home_win_prob appended. MUST run after the dataset
        # loop (it needs team_box, which builds after pbp) and is best-effort --
        # the plain pbp published above is still valid data if this fails.
        # Without it every nightly strips the WP columns off the release, which
        # is what broke the platform's win-probability page in 2026-08. Runs in
        # both modes: the R path publishes the same pbp parquet.
        ( cd python && uv run python -m mbb_data_build.wp_enrich -s "$i" -e "$i" --base ../mbb ) || \
            echo "::warning ::wp_enrich for season $i exited with code $? (non-fatal; release keeps plain pbp)"

        echo "RSCRIPT_RC=$SEASON_RC" > "/tmp/_rscript_rc_${i}"
        # Grep-able terminal line for the season logfile (scrape-log convention).
        echo "season $i EXIT=$SEASON_RC"
        # Commit whatever datasets succeeded even if one step errored -- the
        # per-dataset error handling keeps partial output usable.
        git pull >> /dev/null
        git add mbb/* >> /dev/null
        git pull >> /dev/null
        git add . >> /dev/null
        # Load-bearing subject: downstream tooling parses the years out of it.
        git commit -m "MBB Data Updated (Start: $i End: $i)" || echo "No changes to commit"
        git pull >> /dev/null
        git push >> /dev/null
    } 2>&1 | tee "$TMPLOG"
    RSCRIPT_RC=$(sed 's/RSCRIPT_RC=//' "/tmp/_rscript_rc_${i}" 2>/dev/null)
    rm -f "/tmp/_rscript_rc_${i}"

    # Block is finished and pushed; tee has closed $TMPLOG. Now copy the log
    # into its tracked location and commit/push it on its own.
    cp "$TMPLOG" "$LOGFILE"
    git stash -u --quiet 2>/dev/null || true
    git pull --rebase >> /dev/null || true
    git stash pop --quiet 2>/dev/null || true
    git add "$LOGFILE"
    git commit -m "MBB Data log update (Start: $i End: $i)" >> /dev/null || echo "No log changes to commit"
    git push >> /dev/null
    rm -f "$TMPLOG"

    # Propagate any non-zero exit code so the workflow reports failure.
    # Don't `exit` immediately -- iterate the rest of the requested seasons.
    if [ "${RSCRIPT_RC:-0}" != "0" ]; then
        echo "::error ::At least one creation step for season $i exited with code $RSCRIPT_RC"
        ANY_FAILED=1
    fi
done

# ---- Run summary: updated releases + remaining warnings/errors ----
# Prints a cli summary to the Action log and (when set) writes markdown to
# $GITHUB_STEP_SUMMARY so the run's Summary tab shows what landed and what didn't.
if [ "$LANG_MODE" = "R" ]; then
    Rscript R/run_summary.R -s "$START_YEAR" -e "$END_YEAR" || true
else
    ( cd python && uv run python -m mbb_data_build.summary --logs ../logs -s "$START_YEAR" -e "$END_YEAR" ) || true
fi

if [ "${ANY_FAILED:-0}" != "0" ]; then
    echo "::error ::At least one season's creation step exited non-zero. See per-season logs."
    exit 1
fi
