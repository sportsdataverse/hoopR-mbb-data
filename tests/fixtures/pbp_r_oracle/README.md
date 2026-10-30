# Frozen R pbp oracle

`play_by_play_r_oracle.parquet` is the R (hoopR) play-by-play for four real
2025 MBB games. `tests/mbb_data_build/test_parity_pbp.py` compares the Python
producer against it. Nothing in the daily pipeline writes to this folder.

## Provenance

| | |
|---|---|
| Captured | 2026-10-30 |
| hoopR | 3.2.0, commit `f44190c24c01ee274f7b6c92f32d4abb19432d28` (installed with `R CMD INSTALL` into a scratch library) |
| R | 4.6.1 (2026-06-24 ucrt), Windows; arrow 25.0.1, dplyr 1.2.1, jsonlite 2.0.0 |
| Script | `build_r_oracle.R` (the per-game compile from `R/espn_mbb_01_pbp_creation.R`, without the HTTP reads or the upload) |
| Output | 1,360 rows x 58 columns |

## Raw inputs

`raw/<game_id>.json.gz` holds the ESPN summary payloads, gzipped (`gzip -9 -n`)
and otherwise byte-identical to `hoopR-mbb-raw/mbb/json/final/<game_id>.json`
at raw commit `fc6dab6d7cf4afdd4e3e634655215f97ef7b1417`. They were copied from
that checkout. Nothing was fetched from ESPN.

| game_id | game | sha256 of the uncompressed JSON |
|---|---|---|
| 401721337 | Nebraska at Iowa, 2025-01-07, overtime | `de5228d09f2b35a81fc1627addcae77396d338042e44fdbb460957b45a028ee9` |
| 401721734 | Harvard at Cornell, 2025-02-01 | `61432899bfd0dfce1f3cfa4feed97f141fef2c63afb2d97c72159cadf6e425f6` |
| 401724552 | IU Indianapolis vs Coastal Carolina, 2024-11-22, neutral site | `90fc6fa65475722b6810374968bf707a70f11a82fe69b24a34a6dec49a32adc2` |
| 401746065 | Maryland vs Florida, 2025-03-27, NCAA tournament | `fba4e5d5c7ce63ecc74f8d98af3c7e8dbfbe9bb00f2ac738961121a7304a4204` |

## Rebuilding

Rebuild only on purpose, for example after a deliberate hoopR change. From the repo root:

```sh
R CMD INSTALL --library=<scratch lib> <path to hoopR checkout>
R_LIBS=<scratch lib> Rscript tests/fixtures/pbp_r_oracle/build_r_oracle.R
```

Then update the table above. A parity failure after a rebuild is a real
R-vs-Python difference. Fix it, or list it in the test with a reason.
Do not regenerate the oracle from Python output.
