"""Python producer for the ESPN MBB release datasets.

Parity port of ``hoopR-mbb-data/R/espn_mbb_*_creation.R``. Reshapes the sibling
``hoopR-mbb-raw`` per-game JSON into season-level parquet/csv + manifest and
publishes to the ``espn_mens_college_basketball_*`` release tags (MBB's tag
prefix is the full league name, not ``espn_mbb_*``). R is retained as the
byte-parity oracle.
"""

__all__ = ["config", "ingest", "io", "build", "publish", "reshapers"]
