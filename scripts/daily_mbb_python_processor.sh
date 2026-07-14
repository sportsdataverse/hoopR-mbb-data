#!/bin/bash
# Python equivalent of daily_mbb_R_processor.sh: compiles + publishes the MBB
# datasets via the `mbb_data_build` package (sdv-py producers) instead of the R
# creation scripts. The R script is retained for rollback / shadow comparison.
#
# Crosswalks (mbb_11/12/13) have NO python port and stay on R.
# Usage: bash scripts/daily_mbb_python_processor.sh -s 2025 -e 2025

while getopts s:e: flag
do
    case "${flag}" in
        s) START_YEAR=${OPTARG};;
        e) END_YEAR=${OPTARG};;
    esac
done

if [ -z "$START_YEAR" ] || [ -z "$END_YEAR" ]; then
    echo "Usage: $0 -s <start_year> -e <end_year>"
    exit 1
fi

# CI has no local hoopR-mbb-raw checkout -- read raw over HTTP (the reason the
# python builders are dual-mode Path|str). Override with HOOPR_MBB_RAW_ROOT.
RAW_ROOT="${HOOPR_MBB_RAW_ROOT:-https://raw.githubusercontent.com/sportsdataverse/hoopR-mbb-raw/main}"

# Build order encodes the cross-dataset dependencies:
#   pbp        -> shots (shots derive from the built pbp parquet)
#   pbp/team_box/player_box -> schedules (PBP/box availability flags)
#   player_box -> player_season_stats (identity lookup from the built player_box)
PY_DATASETS=(
    pbp
    team_box
    player_box
    schedules
    shots
    rosters
    player_season_stats
    team_season_stats
    standings
    game_rosters
    officials
)
# No python port -- crosswalks stay on R.
R_CROSSWALK=(
    R/mbb_11_team_crosswalk_creation.R
    R/mbb_12_schedule_crosswalk_creation.R
    R/mbb_13_player_crosswalk_creation.R
)

mkdir -p logs
for i in $(seq "${START_YEAR}" "${END_YEAR}")
do
    LOGFILE="logs/hoopR_mbb_data_logfile_${i}.log"
    TMPLOG=$(mktemp "/tmp/hoopR_mbb_data_logfile_${i}.XXXXXX.log")
    echo "=== Processing MBB data (python) for season $i ==="
    {
        git pull >> /dev/null
        git config --local user.email "action@github.com"
        git config --local user.name "Github Action"
        SEASON_RC=0
        for DS in "${PY_DATASETS[@]}"
        do
            # Run inside python/ so the flat mbb_data_build package is importable
            # (it is not pip-installed; found via CWD/pythonpath). --base ../mbb
            # writes into the repo-root mbb/ tree.
            ( cd python && uv run python -m mbb_data_build \
                --dataset "$DS" -s "$i" -e "$i" --base ../mbb --raw-root "$RAW_ROOT" --publish ) || {
                rc=$?
                echo "::warning ::mbb_data_build $DS for season $i exited with code $rc"
                SEASON_RC=$rc
            }
        done
        for SCRIPT in "${R_CROSSWALK[@]}"
        do
            # Crosswalks build from LIVE ESPN+Torvik+Fox sources and are known-fragile
            # (segfaults/timeouts on external flakiness). Best-effort: a crosswalk
            # failure warns but does NOT fail the run -- the 11 core python datasets
            # are the daily deliverable and publish independently above.
            Rscript "$SCRIPT" -s $i -e $i || echo "::warning ::$SCRIPT for season $i exited with code $? (crosswalk; non-fatal, live external source)"
        done
        echo "RSCRIPT_RC=$SEASON_RC" > "/tmp/_rscript_rc_${i}"
        git pull >> /dev/null
        git add mbb/* >> /dev/null
        git pull >> /dev/null
        git add . >> /dev/null
        git commit -m "MBB Data Updated (Start: $i End: $i)" || echo "No changes to commit"
        git pull >> /dev/null
        git push >> /dev/null
    } 2>&1 | tee "$TMPLOG"
    RSCRIPT_RC=$(cat "/tmp/_rscript_rc_${i}" 2>/dev/null | sed 's/RSCRIPT_RC=//')
    rm -f "/tmp/_rscript_rc_${i}"

    cp "$TMPLOG" "$LOGFILE"
    git stash -u --quiet 2>/dev/null || true
    git pull --rebase >> /dev/null || true
    git stash pop --quiet 2>/dev/null || true
    git add "$LOGFILE"
    git commit -m "MBB Data log update (Start: $i End: $i)" >> /dev/null || echo "No log changes to commit"
    git push >> /dev/null
    rm -f "$TMPLOG"

    if [ "${RSCRIPT_RC:-0}" != "0" ]; then
        echo "::error ::At least one builder for season $i exited with code $RSCRIPT_RC"
        ANY_FAILED=1
    fi
done

Rscript R/run_summary.R -s "$START_YEAR" -e "$END_YEAR" || true

if [ "${ANY_FAILED:-0}" != "0" ]; then
    echo "::error ::At least one season's builder exited non-zero. See per-season logs."
    exit 1
fi
