"""models/manifest.yaml is the single home for the model/stage list (Track C step 2).

File-based per-row biting guards: manifest ↔ numbered stage scripts ↔ REGISTRY.md.
"""
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "models" / "manifest.yaml"
REGISTRY = ROOT / "models" / "REGISTRY.md"


def _models() -> dict:
    return yaml.safe_load(MANIFEST.read_text(encoding="utf-8"))["suites"]["mbb"]["models"]


def test_manifest_parses_and_driver_exists():
    doc = yaml.safe_load(MANIFEST.read_text(encoding="utf-8"))
    assert (ROOT / doc["driver"]).is_file()


def test_stages_and_manifest_agree_bidirectionally():
    files = {p.stem for p in (ROOT / "python").glob("mbb_model_[0-9][0-9]_*.py")}
    manifest = {Path(m["stage"]).stem for m in _models().values()}
    assert files == manifest, f"files-only={files - manifest}, manifest-only={manifest - files}"


def test_each_stage_exists_wraps_its_target_and_has_main():
    for name, m in _models().items():
        stage = ROOT / m["stage"]
        assert stage.is_file(), f"{name} stage missing"
        src = stage.read_text(encoding="utf-8")
        assert "def main(" in src, f"{name} stage has no main()"
        assert m["wraps_token"] in src, f"{name} stage does not wrap {m['wraps_token']!r}"


def test_registry_names_every_tag_and_wiring_exists():
    registry = REGISTRY.read_text(encoding="utf-8")
    for name, m in _models().items():
        assert m["release_tag"] in registry, f"{name} tag not in REGISTRY.md"
        if m.get("wired_via"):
            assert (ROOT / m["wired_via"]).is_file(), f"{name} wiring file missing"
