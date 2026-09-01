"""Stage 02 — MBB per-player box Plus/Minus (player value).

Thin numbered entry over ``mbb_model_publish player-value``; args forward verbatim (injects the ``player-value`` subcommand).
Compute-on-demand (engines in sdv-py); card sidecar is the per-publish ledger.
Usage::

    python -m mbb_model_02_player_value --start 2025 --end 2026
    scripts/mbb_models.sh 02
"""
from __future__ import annotations

import sys


def main(argv: list[str] | None = None) -> int:
    from mbb_model_publish.cli import main as _main

    argv = list(argv) if argv is not None else sys.argv[1:]
    return _main(["player-value", *argv])


if __name__ == "__main__":
    raise SystemExit(main())
