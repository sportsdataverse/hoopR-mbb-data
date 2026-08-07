# `schedule_crosswalk`

`schedule_crosswalk` reshaper -- release tag [`mbb_crosswalk`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mbb_crosswalk).

| | |
|---|---|
| **Builder** | [`python/espn_mbb_12_schedule_crosswalk_creation.py`](../../python/espn_mbb_12_schedule_crosswalk_creation.py) |
| **Release tag** | [`mbb_crosswalk`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mbb_crosswalk) |
| **File stem** | `mbb_schedule_crosswalk_{season}.{parquet,csv,rds}` |
| **Manifested** | no (no load_mbb_*_manifest() loader yet) |
| **Last published** | — (newest release asset) |
| **Tag created** | — |
| **Release assets** | — |

## Automation

`.github/workflows/daily_mbb.yml` -- daily cron, running `scripts/daily_mbb_R_processor.sh` (R, the current daily producer). `scripts/daily_mbb_python_processor.sh` runs the parity-tested Python build in `mbb_data_build` but does not yet drive daily CI.

## Columns

_No committed parquet found locally to derive a schema from._

## Coverage

_Coverage is tracked per release asset on [`mbb_crosswalk`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mbb_crosswalk)._
