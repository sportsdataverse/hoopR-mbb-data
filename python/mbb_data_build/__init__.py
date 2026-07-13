"""Python producer for the ESPN MBB release datasets.

Parity port of ``hoopR-mbb-data/R/espn_mbb_*_creation.R``. Reshapes the sibling
``hoopR-mbb-raw`` per-game JSON into season-level parquet/csv + manifest and
publishes to the ``espn_mbb_*`` release tags. R is retained
as the byte-parity oracle.
"""

__all__ = ["config", "ingest", "io", "build", "publish", "reshapers"]
