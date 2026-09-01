# Model registry

One row per model surface this repo operates (Track C step 1). The WP model
itself is trained and bundled in **sdv-py** (the MBB rule-era WP suite); this
repo's surface is the ENRICHMENT — applying it to the published season pbp in
place, which is how the model reaches consumers (`in_published_data` = the
`espn_mens_college_basketball_pbp` assets themselves).
`tests/test_model_registry.py` keeps this table in lockstep.

| model | artifact(s) | release tag | training data | fitting script | gates at publish | last retrain | cadence |
|---|---|---|---|---|---|---|---|
| MBB per-play win probability (enrichment of the published pbp) | WP columns added in place to `play_by_play_{season}` assets, 2003–2026, every original column preserved | `espn_mens_college_basketball_pbp` (no separate model tag) | sdv-py MBB WP training corpus (rule-era models; see sdv-py) | `python/mbb_model_03_wp_enrich.py` (wraps `mbb_data_build/wp_enrich.py`) via `scripts/daily_mbb_data_processor.sh` (post-publish step of the daily data cron) | oracle gates live with the model in sdv-py; enrichment invariant: every original column preserved, WP columns re-added after each pbp publish (the nightly publish otherwise silently strips them — that incident is why `wp_enrich` runs post-publish) | model: see sdv-py; enrichment re-applied per run | in-season daily 13:00 UTC (Nov–Apr, `mbb_models_cron.yml`) |

Note: the fitted WP boosters are registered where they are trained (sdv-py);
this registry deliberately covers only what this repo owns — the enrichment
op and its schedule.

## Operability (Track C steps 2–6)

- `models/manifest.yaml` — single home for the model/stage list (guarded by `tests/test_model_manifest.py`).
- One model = one numbered pipeline, flat in `python/` beside the data stages; run subsets with `scripts/mbb_models.sh`.
- Compute-on-demand / enrichment surfaces: no fitted artifacts to commit, no fingerprint skip (living upstream inputs), card sidecars carry per-publish metadata.
- `mbb_ratings` + `mbb_player_value` rows: see `models/manifest.yaml` (stages 01/02 wrap `mbb_model_publish ratings` / `player-value`, wired via `mbb_models_cron.yml`); engines + gates live in sdv-py, card sidecars per publish.
