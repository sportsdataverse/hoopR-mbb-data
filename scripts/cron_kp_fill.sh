#!/usr/bin/env bash
# Droplet cron entry: fill KenPom ids into the current-season MBB schedule + player crosswalks.
# daily_mbb.yml builds and publishes them on GitHub runners, which have no hoopR-kp-data
# checkout (and barttorvik.com 403s the droplet, so the full build cannot move here).
# kenpom.py carries the last published ids forward there; this step adds the new ones
# from the droplet's hoopR-kp-data capture (scripts/cron_capture_current.sh, earlier
# the same morning) with `python -m mbb_data_build.kp_fill`, then commits + publishes.
#
#   bash scripts/cron_kp_fill.sh [-s YYYY] [-n]    # -n = dry run into a temp tree, no commit
#
# Exits 0 without filling before the season's crosswalks or KenPom capture exist.
set -uo pipefail
cd "$(dirname "$0")/.."
echo "=== cron_kp_fill $(date -u +%FT%TZ)"
M=$((10#$(date +%m))); YR=$(date +%Y); [ "$M" -ge 10 ] && YR=$((YR + 1)); DRY=0   # END year
while getopts s:n flag; do
  case "$flag" in s) YR=$OPTARG;; n) DRY=1;; *) echo "usage: $0 [-s YYYY] [-n]" >&2; exit 2;; esac
done
fail() { echo "::error ::$*"; echo "EXIT=1"; exit 1; }

# Hold the git_pull sweep's lock (it would autostash this run's outputs); git children get
# 9>&- so a daemonized gc / credential cache cannot keep the lock after this exits.
exec 9>/tmp/git_pull_sdv.lock
flock -w 3600 9 || fail "git_pull sweep held its lock for 1h"
if [ "$DRY" = 0 ]; then
  branch=$(git rev-parse --abbrev-ref HEAD)
  if [ "$branch" != main ] || [ -n "$(git status --porcelain --untracked-files=no)" ]; then
    git status --short --untracked-files=no | head -20
    fail "checkout is on '$branch' or has tracked changes; refusing to fill"
  fi
  git pull -q --ff-only 9>&- || fail "git pull --ff-only failed"
fi

base="../mbb"; mode=--publish
if [ "$DRY" = 1 ]; then base=$(mktemp -d); mode=--dry-run; fi
export PYTHONUNBUFFERED=1 PYTHONIOENCODING=utf-8
(cd python && uv run --quiet python -m mbb_data_build.kp_fill -s "$YR" --base "$base" "$mode") 9>&- \
  || fail "kp_fill $YR failed"
[ "$DRY" = 1 ] && { echo "dry run: wrote $base/crosswalk (only if ids changed), no commit"; echo "EXIT=0"; exit 0; }

# .rds is gitignored: commit the parquet + manifests only.
git add -- mbb/crosswalk/parquet mbb/crosswalk/*_in_data_repo.csv
git diff --cached --quiet && { echo "KenPom ids unchanged"; echo "EXIT=0"; exit 0; }
git commit -q -m "MBB Data Updated (Start: $YR End: $YR)" 9>&- || fail "commit failed"
for attempt in 1 2 3; do   # daily_mbb.yml may push meanwhile; rebase --merge (parquet blobs)
  git push -q origin HEAD 9>&- && { echo "pushed MBB $YR KenPom ids"; echo "EXIT=0"; exit 0; }
  git fetch -q origin main 9>&- && git rebase -q --merge origin/main 9>&- \
    || { git rebase --abort 2>/dev/null; fail "cannot rebase onto origin/main"; }
done
fail "push rejected 3 times"
