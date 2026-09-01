# MBB opponent-adjusted team ratings

## Overview

Per-season team ratings on the `mbb_ratings` tag: offensive/defensive
efficiency adjusted for opponent quality via the sdv-py MBB prediction
stack's iterative adjustment (the em-scale fixed-point engine), so a team's
number reflects who it played, not just what it scored.

## Data & methodology

Inputs are the published season pbp/box assets. The engine solves the
opponent-adjustment fixed point over the season-to-date game matrix; ratings
are recomputed (not incrementally updated) on every run, so late corrections
to the underlying data propagate. Engines and their oracle gates live in
sdv-py where they are trained; each publish writes a card sidecar with
per-run provenance.

## Evaluation

### Ratings (2025)

700 teams on `mbb_ratings`.

![Top 25 ratings](figures/ratings_top25_2025.png)



## Reproducibility

`scripts/mbb_models.sh 01` → `python/mbb_model_01_ratings.py` (wired via
`mbb_models_cron.yml`). Card: [`mbb_models_eval_card.json`](mbb_models_eval_card.json).

## Limitations

A pure efficiency model: no injury/roster awareness, and early-season
estimates lean on thin game matrices until the fixed point stabilizes.

## Avenues for improvement & open issues

- **Preseason priors** — blend the recruiting/returning-production prior into
  early-season ratings instead of starting from a flat matrix.
- **Home/travel modeling** — altitude and travel distance are unmodeled.
- **Known issue:** Spearman-style external checks are scale-blind; the level
  bands that catch scale bugs live in the sdv-py gates, not here.
