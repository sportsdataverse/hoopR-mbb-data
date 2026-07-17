"""Release-asset uploads via the gh CLI (pattern path, copied from the family template)."""

from __future__ import annotations

import subprocess
from pathlib import Path

GH_TIMEOUT_SECONDS = 300

# Release-notes body used when auto-creating a missing release. Keyed by tag;
# falls back to a generic note for any other tag.
_RELEASE_BODY = {
    "mbb_ratings": (
        "Men's college basketball opponent-adjusted team ratings, one row per "
        "team per season: AdjO/AdjD/AdjEM/AdjTempo with dense ranks and an "
        "AdjEM z-score. Built by sdv-py `mbb_team_ratings()` over the released "
        "ESPN schedule + team boxscores."
    ),
    "mbb_player_value": (
        "Men's college basketball per-player-season box Plus/Minus "
        "(offense/defense/total, team-constrained so minutes-weighted player "
        "scores sum to the team's adjusted efficiency margin). Built by sdv-py "
        "`mbb_box_bpm()` over the released ESPN player boxscores."
    ),
}


def _gh_runner(args: list) -> None:
    subprocess.run(["gh", *args], check=True, timeout=GH_TIMEOUT_SECONDS)


def _gh_release_exists(tag: str, repo: str) -> bool:
    """True if a GitHub release for ``tag`` already exists on ``repo``."""
    r = subprocess.run(
        ["gh", "release", "view", tag, "--repo", repo],
        capture_output=True,
        timeout=GH_TIMEOUT_SECONDS,
    )
    return r.returncode == 0


def upload_artifacts(
    artifacts_dir,
    tag: str,
    repo: str,
    *,
    pattern: str,
    dry_run: bool = False,
    runner=None,
    exists_check=None,
) -> dict:
    """Upload ``artifacts_dir.glob(pattern)`` (sorted) to the ``tag`` release.

    The release is created if it does not already exist (``gh release upload``
    does not create one), so a single call is self-sufficient. Uploads are one
    ``gh release upload`` per file with ``--clobber`` -- never a multi-file
    glob, which silently drops large assets. ``runner`` and ``exists_check``
    are injectable for hermetic testing.
    """
    run = runner or _gh_runner
    exists = exists_check or _gh_release_exists
    files = sorted(Path(artifacts_dir).glob(pattern))
    created_release = False
    if dry_run:
        print(f"[dry-run] would ensure release {repo}:{tag} exists")
    elif not exists(tag, repo):
        body = _RELEASE_BODY.get(tag, f"{tag} (auto-created by mbb_model_publish).")
        run(["release", "create", tag, "--repo", repo, "--title", tag, "--notes", body])
        created_release = True
    uploaded = 0
    for f in files:
        if dry_run:
            print(f"[dry-run] would upload {f} -> {repo}:{tag}")
            continue
        run(["release", "upload", tag, str(f), "--repo", repo, "--clobber"])
        uploaded += 1
    return {
        "uploaded": uploaded,
        "files": [str(f) for f in files],
        "tag": tag,
        "created_release": created_release,
    }
