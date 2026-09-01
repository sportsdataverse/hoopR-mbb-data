"""Stage 03 — MBB per-play WP enrichment of the published pbp.

Thin numbered entry over ``mbb_data_build.wp_enrich``; args forward verbatim.
Runs post-publish in the daily data processor (the nightly publish otherwise strips the WP columns).
Usage::

    python -m mbb_model_03_wp_enrich -s 2026 -e 2026 --base ../mbb
    scripts/mbb_models.sh 03
"""
from __future__ import annotations

import sys


def main(argv: list[str] | None = None) -> int:
    from mbb_data_build.wp_enrich import main as _main

    argv = list(argv) if argv is not None else sys.argv[1:]
    return _main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
