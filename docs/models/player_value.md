# MBB player value (box Plus/Minus)

## Overview

Per-player box Plus/Minus on the `mbb_player_value` tag (`box_obpm` /
`box_dbpm` / `box_bpm`): a box-score value model over the published
player/team season stats, sharing the design of the MBB player-value spine
in sdv-py (oracle-gated where trained).

## Data & methodology

Box-score features are regressed onto team-level results to apportion value;
offensive and defensive components are estimated separately and summed.
Compute-on-demand: every run recomputes from the current published season
assets, and the publish writes a card sidecar.

## Evaluation

### Player value (2025)

9,805 players on `mbb_player_value`.

![Player value distribution](figures/player_value_distribution_2025.png)



## Reproducibility

`scripts/mbb_models.sh 02` → `python/mbb_model_02_player_value.py`.

## Limitations

Box-score-only: value that never reaches the box score (screening, defensive
attention) is invisible; low-minute players are noisy by construction.

## Avenues for improvement & open issues

- **Blend with on/off** — box Plus/Minus and the league-wide RAPM
  (ncaa hoops repos) measure different things; a stabilized hybrid (SPM-prior
  RAPM, as the impact suite does) is the natural next step.
- **Known issue:** no minutes floor is enforced in the published frame —
  consumers must filter low-minute noise themselves.
