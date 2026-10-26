#!/usr/bin/env bash
# Droplet cron entry: fill Fox / Yahoo ids into the committed MBB schedule + player crosswalks
# from the parsed box-score history in hoopR-mbb-alt-data (bin/alt_daily.sh parses it; Yahoo's
# only roster endpoint is today's roster, so yahoo_player_id can come from nowhere else).
# daily_mbb.yml rebuilds the crosswalks on GitHub runners and alt_fill.carry_forward keeps these
# ids there; this step adds the new ones with `python -m mbb_data_build.alt_fill`, then commits +
# publishes. Template: scripts/cron_kp_fill.sh.
#
#   bash scripts/cron_alt_fill.sh -s YYYY [-e YYYY] [-n]   # -n = dry run into a temp tree, no commit
#
# Exits 0 without filling when a season has no parsed alt season or no crosswalk.
# Alt root: $HOOPR_MBB_ALT_DATA_ROOT, else the sibling hoopR-mbb-alt-data checkout.
set -uo pipefail
cd "$(dirname "$0")/.."
echo "=== cron_alt_fill $(date -u +%FT%TZ)"
M=$((10#$(date +%m))); YR=$(date +%Y); [ "$M" -ge 10 ] && YR=$((YR + 1)); END=""; DRY=0   # END year
while getopts s:e:n flag; do
  case "$flag" in
    s) YR=$OPTARG;; e) END=$OPTARG;; n) DRY=1;;
    *) echo "usage: $0 -s YYYY [-e YYYY] [-n]" >&2; exit 2;;
  esac
done
END=${END:-$YR}
fail() { echo "::error ::$*"; echo "EXIT=1"; exit 1; }

# Hold the git_pull sweep's lock (it would autostash this run's outputs); git children get
# 9>&- so a daemonized gc / credential cache cannot keep the lock after this exits. A dry run
# writes outside the tree, so it neither takes the lock nor waits on it.
if [ "$DRY" = 0 ]; then
  exec 9>/tmp/git_pull_sdv.lock
  flock -w 3600 9 || fail "git_pull sweep held its lock for 1h"
  branch=$(git rev-parse --abbrev-ref HEAD)
  if [ "$branch" != main ] || [ -n "$(git status --porcelain --untracked-files=no)" ]; then
    git status --short --untracked-files=no | head -20
    fail "checkout is on '$branch' or has tracked changes; refusing to fill"
  fi
  git pull -q --ff-only 9>&- || fail "git pull --ff-only failed"
fi

out=(); mode=(--publish)
if [ "$DRY" = 1 ]; then out=(--out "$(mktemp -d)"); mode=(); fi
export PYTHONUNBUFFERED=1 PYTHONIOENCODING=utf-8
(cd python && uv run --quiet python -m mbb_data_build.alt_fill -s "$YR" -e "$END" --base ../mbb \
  "${out[@]}" "${mode[@]}") 9>&- || fail "alt_fill $YR-$END failed"
[ "$DRY" = 1 ] && { echo "dry run: wrote ${out[1]}/crosswalk (only if ids changed), no commit, no publish"; echo "EXIT=0"; exit 0; }

# .rds is gitignored: commit the parquet + manifests only.
git add -- mbb/crosswalk/parquet mbb/crosswalk/*_in_data_repo.csv
git diff --cached --quiet && { echo "Fox / Yahoo ids unchanged"; echo "EXIT=0"; exit 0; }
git commit -q -m "MBB Data Updated (Start: $YR End: $END)" 9>&- || fail "commit failed"
for attempt in 1 2 3; do   # daily_mbb.yml may push meanwhile; rebase --merge (parquet blobs)
  git push -q origin HEAD 9>&- && { echo "pushed MBB $YR-$END Fox / Yahoo ids"; echo "EXIT=0"; exit 0; }
  git fetch -q origin main 9>&- && git rebase -q --merge origin/main 9>&- \
    || { git rebase --abort 2>/dev/null; fail "cannot rebase onto origin/main"; }
done
fail "push rejected 3 times"
