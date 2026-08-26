#!/usr/bin/env bash
# hoopR#23 (2026-08-26): offline rebuild (NO publish) of the player_box
# seasons affected by the ESPN dual-team dupe-athlete defect, then gate.
# Publish is a separate, gated step: 20260826_hoopr23_republish_affected_seasons.sh
set -uo pipefail
cd "$(dirname "$0")/../.."

RAW_ROOT="${HOOPR_MBB_RAW_ROOT:-C:/Users/saiem/Documents/GitHub-Data/sdv-dev/hoopR-dev/hoopR-mbb-raw}"
SEASONS=(2014 2015 2017 2018 2020 2021 2022 2025)

for y in "${SEASONS[@]}"; do
    echo "=== $(date -u +%FT%TZ) rebuild player_box $y ==="
    ( cd python && uv run python -m mbb_data_build \
        --dataset player_box -s "$y" -e "$y" --base ../mbb --raw-root "$RAW_ROOT" ) || {
        echo "BUILD FAILED for $y"; echo "EXIT=1"; exit 1
    }
done

echo "=== $(date -u +%FT%TZ) gating ==="
uv run python ops/oneoff/20260826_hoopr23_player_box_dedupe_gate.py "${SEASONS[@]}"
rc=$?
echo "EXIT=$rc"
exit $rc
