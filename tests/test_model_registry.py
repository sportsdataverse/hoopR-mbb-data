"""models/REGISTRY.md carries the WP-enrichment row (Track C guard).

This repo's model surface is the ENRICHMENT of the published pbp, not a
fitted artifact of its own — the row must name the enrichment script and the
pbp tag it writes into.
"""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "models" / "REGISTRY.md"


def _rows() -> list[str]:
    text = REGISTRY.read_text(encoding="utf-8")
    return [ln for ln in text.splitlines() if ln.startswith("|") and "---" not in ln]


def test_registry_exists():
    assert REGISTRY.is_file(), "models/REGISTRY.md is missing"


def test_wp_enrich_row_present():
    row = next((r for r in _rows() if "wp_enrich" in r), None)
    assert row, "no registry row naming wp_enrich"
    assert "espn_mens_college_basketball_pbp" in row, "row must name the pbp tag it enriches"


def test_enrich_script_exists():
    assert (ROOT / "python" / "mbb_data_build" / "wp_enrich.py").is_file()
