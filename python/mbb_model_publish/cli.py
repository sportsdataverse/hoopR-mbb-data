from __future__ import annotations

import argparse

from .artifacts import upload_artifacts
from .builders import (
    build_player_value,
    build_ratings,
    write_player_value_card,
    write_ratings_card,
)


def _seasons(spec: str) -> list[int]:
    """Parse ``2002:2026`` or ``2025`` into a season list."""
    if ":" in spec:
        start, end = spec.split(":", 1)
        return list(range(int(start), int(end) + 1))
    return [int(spec)]


def _add_common(p: argparse.ArgumentParser, default_out: str, default_tag: str) -> None:
    p.add_argument(
        "--seasons",
        required=True,
        help="a season (2025) or an inclusive range (2002:2026); hoopR end-year convention",
    )
    p.add_argument("--out", default=default_out)
    p.add_argument("--tag", default=default_tag)
    p.add_argument("--repo", default="sportsdataverse/sportsdataverse-data")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument(
        "--build-only",
        action="store_true",
        help="write parquet + card, skip the upload",
    )


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="mbb_model_publish")
    sub = ap.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("ratings", help="build + publish opponent-adjusted team ratings")
    _add_common(r, "out/mbb_ratings", "mbb_ratings")

    v = sub.add_parser("player-value", help="build + publish per-player box Plus/Minus")
    _add_common(v, "out/mbb_player_value", "mbb_player_value")

    return ap


def _run(args, builder, card_writer, stem: str) -> int:
    results = builder(_seasons(args.seasons), args.out)
    card_writer(results, args.out)
    total = sum(r["rows"] for r in results)
    if args.build_only:
        print(f"{stem}: built seasons={len(results)} rows={total} -> {args.out} (build-only)")
        return 0
    res = upload_artifacts(
        args.out,
        args.tag,
        args.repo,
        pattern=f"{stem}_*.*",
        dry_run=args.dry_run,
    )
    created = " (created release)" if res.get("created_release") else ""
    print(
        f"publish: seasons={len(results)} rows={total} uploaded={res['uploaded']} "
        f"-> {args.repo}:{res['tag']}" + created + (" (dry-run)" if args.dry_run else "")
    )
    return 0


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    if args.cmd == "ratings":
        return _run(args, build_ratings, write_ratings_card, "mbb_ratings")
    if args.cmd == "player-value":
        return _run(args, build_player_value, write_player_value_card, "mbb_player_value")
    return 0
