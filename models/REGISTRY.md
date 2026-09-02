# Model registry

One row per model surface this repo operates (Track C step 1). The WP model
itself is trained and bundled in **sdv-py** (the MBB rule-era WP suite); this
repo's surface is the ENRICHMENT — applying it to the published season pbp in
place, which is how the model reaches consumers (`in_published_data` = the
`espn_mens_college_basketball_pbp` assets themselves).
`tests/test_model_registry.py` keeps this table in lockstep.

| model | artifact(s) | release tag | training data | fitting script | gates at publish | last retrain | cadence |
|---|---|---|---|---|---|---|---|
| MBB per-play win probability (enrichment of the published pbp) | WP columns added in place to `play_by_play_{season}` assets, every original column preserved. Contract 2003–2026; **measured on the release 2026-09-01: only 2024–2026 carry the columns** — 2003–2023 were stripped by the 2026-08-26 history republish and await `mbb_model_03_wp_enrich -s 2003 -e 2023` | `espn_mens_college_basketball_pbp` (no separate model tag) | one XGBoost in-game booster over game state (score margin, seconds left, its square root, pregame logit, possession), fit on a SINGLE season (2023) -- not a rule-era family; trained and registered in sdv-py | `python/mbb_model_03_wp_enrich.py` (wraps `mbb_data_build/wp_enrich.py`) via `scripts/daily_mbb_data_processor.sh` (the pbp asset's ONLY publisher -- the pbp build stage writes the tree and never uploads; the enrichment reads pbp/schedules/team_box from the tree, appends the WP columns, and publishes parquet+csv+rds) | oracle gates live with the model in sdv-py; enrichment invariant: every original column preserved and the row count unchanged; **publish guard** `publish.assert_wp_enriched` (asserted on the parquet FILE about to upload, by every caller incl. dry runs) refuses a pbp asset missing `pregame_home_prob`/`home_win_prob` or below a 0.999 finite-rate floor -- observed 2026-09-01: 1.0 on 2024/2025/2026, while 2003-2023 had lost the columns entirely to the 2026-08-26 history republish (the strip incident; the guard would have refused it) | model: see sdv-py; enrichment re-applied per run | in-season daily 13:00 UTC (Nov–Apr, `mbb_models_cron.yml`) |

Note: the fitted WP boosters are registered where they are trained (sdv-py);
this registry deliberately covers only what this repo owns — the enrichment
op and its schedule.

## Publish gates & derived columns (added 2026-09-01; every constant cites the observation that set it)

- **`mbb_ratings` — absolute level-band gate** (`mbb_model_publish.builders.assert_ratings_level`), the scale
  check beside sdv-py's rank (Spearman) gates, which are blind to any monotone rescale. Over the qualified subset
  (teams with `games >= 10` — the D1 core; the full frame carries every opponent ever seen), the season must have
  mean `adj_o` and mean `adj_d` in [95, 118], mean `adj_em` in [-8, 8], sd `adj_em` in [10, 22], mean `adj_tempo`
  in [60, 76], and no non-finite value; applied once >= 150 teams qualify (~Dec 10), logged as not-applied before.
  Observed on the published 2006–2026 assets + 2024/2025/2026 in-season engine snapshots: qualified teams 334–366
  (153+ from Dec 10), mean adj_o 101.9–111.1, mean adj_d 100.7–108.5, mean adj_em 0.09–6.0, sd adj_em 13.2–16.7,
  mean adj_tempo 65.1–70.0. Verified on real assets: 2006/2014/2021/2025/2026 pass; **the published
  `mbb_ratings_2011.parquet` (604/604 teams NaN) is refused.** Per-season record lands in the card
  (`gates_by_season`).
- **`mbb_player_value` — additive `qualified` flag** (`min >= 300`; `builders.QUALIFIED_MIN_MINUTES`), never a
  filter: every published column and row is preserved. Derived from the published 2014–2026 assets: sd(box_bpm)
  by minutes bin 7.17 (0–25) → 4.78 (100–150) → 4.23 (250–300) → 4.08 (300–350) → 3.73 (600–800) → 3.43
  (1000–1400); 300 is the first bin within 10% of the 600–800 plateau; same-player YoY r 0.42 (all) / 0.767
  (>= 300) / 0.785 (>= 500); >= 300 keeps 88.2% of 2026 minutes (31.2% of rows). The engine's own fit floor
  (artifact `min_minutes` = 150) governs team-sum weights only.
- **`mbb_player_value_coefficients.json`** (additive asset on the same tag; `builders.write_player_value_coefficients`):
  the fitted box-BPM artifact — 16 `feature_cols`, `obpm_coef`/`dbpm_coef` as [intercept, *slopes] on standardized
  z-clipped features (so |slope| = BPM per SD = coefficient importance), `feature_mean`/`feature_sd`, `lambda_o`/`lambda_d`
  (300/100), `min_minutes` 150, `z_clip` 4, `train_seasons` [2025, 2026] — plus the sportsdataverse version, artifact
  sha256 and write time.
- **`espn_mens_college_basketball_pbp` — WP publish guard**: see the row above (`publish.assert_wp_enriched`).

## Operability (Track C steps 2–6)

- `models/manifest.yaml` — single home for the model/stage list (guarded by `tests/test_model_manifest.py`).
- One model = one numbered pipeline, flat in `python/` beside the data stages; run subsets with `scripts/mbb_models.sh`.
- Compute-on-demand / enrichment surfaces: no fitted artifacts to commit, no fingerprint skip (living upstream inputs), card sidecars carry per-publish metadata.
- `mbb_ratings` + `mbb_player_value` rows: see `models/manifest.yaml` (stages 01/02 wrap `mbb_model_publish ratings` / `player-value`, wired via `mbb_models_cron.yml`); engines + gates live in sdv-py, card sidecars per publish.
