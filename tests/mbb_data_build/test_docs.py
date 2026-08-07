"""Offline smoke test for the D40/D43 docs generator (mbb_data_build.docs).

Runs against the real repo tree (docs.REPO_ROOT resolves to the actual
checkout, same as nba_data_build's equivalent test) rather than a synthetic
fixture -- the generator's whole job is to read real committed files
(README.md/CLAUDE.md, the committed mbb/ parquet tree), so a fixture would
just test the fixture. --no-live everywhere: no `gh` calls in CI.
"""

from __future__ import annotations

from mbb_data_build import docs


def test_builder_covers_exactly_the_registry():
    assert set(docs.BUILDER) == set(docs.REGISTRY)
    assert set(docs.PAGES) == set(docs.REGISTRY)


def test_column_table_renders_without_crashing_for_every_dataset():
    for dataset in docs.PAGES:
        table = docs.column_table(dataset)
        assert table  # either a real schema table or the honest placeholder


def test_dataset_page_is_well_formed():
    page = docs.dataset_page("pbp", live=False)
    assert page.startswith("# `pbp`")
    assert "## Columns" in page
    assert "## Coverage" in page


def test_generate_check_is_idempotent(tmp_path, monkeypatch):
    """generate() then generate(check=True) must agree -- no ratchet, no drift."""
    monkeypatch.setattr(docs, "DOCS_DIR", tmp_path / "datasets")
    monkeypatch.setattr(docs, "REPO_ROOT", tmp_path.parent)  # no README/CLAUDE.md here
    assert docs.generate(check=False, live=False) == 0
    assert docs.generate(check=True, live=False) == 0


def test_replace_block_is_idempotent():
    text = "# Title\n\nsome prose\n"
    once = docs._replace_block(text, "row1")
    twice = docs._replace_block(once, "row1")
    assert once == twice
    assert docs.BEGIN in once and docs.END in once
