"""Stage 11 -- team_crosswalk.

Mirrors ``R/mbb_11_team_crosswalk_creation.R`` -- same stage number, same dataset.

Thin shim over the tested build package: the pipeline logic lives in
``mbb_data_build``; this file exists so the stage sequence is readable from a
directory listing. It lines up with ``R/mbb_11_team_crosswalk_creation.R``.

Equivalent to::

    python -m mbb_data_build --dataset team_crosswalk -s <start> -e <end>
"""

from __future__ import annotations

import sys

from mbb_data_build.cli import main

DATASET = "team_crosswalk"

if __name__ == "__main__":
    # DATASET is appended, not prepended: argparse takes the last value for a
    # single-value option, so a stray --dataset on the command line cannot make
    # stage 11 build something other than team_crosswalk.
    sys.exit(main([*sys.argv[1:], "--dataset", DATASET]))
