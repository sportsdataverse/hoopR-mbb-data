"""Guards on the daily driver shell script (the WBB twin carries the same test)."""

from pathlib import Path

DAILY_SH = Path(__file__).resolve().parents[1] / "scripts" / "daily_mbb_data_processor.sh"


def test_pbp_is_published_only_by_the_enrichment_stage():
    """The pbp build stage must not pass --publish; wp_enrich owns that asset."""
    sh = DAILY_SH.read_text(encoding="utf-8")
    assert '[ "$ds" = "pbp" ] && pub=""' in sh
    assert "mbb_model_03_wp_enrich" in sh


def test_wp_enrich_is_gated_on_every_input_it_reads():
    """wp_enrich reads pbp + schedules + team_box out of the tree.

    A failed auxiliary build leaves the PREVIOUS run's parquet in place, so
    gating only on pbp let enrichment run on stale schedules/team_box and
    publish them as fresh. The guard has to cover all three.
    """
    sh = DAILY_SH.read_text(encoding="utf-8")
    assert "PBP_RC" not in sh, "the pbp-only guard is back; wp_enrich needs all three inputs"
    assert 'case "$ds" in pbp|schedules|team_box) WP_INPUT_RC=$rc;; esac' in sh
    assert "*01_pbp*|*02_team_box*) WP_INPUT_RC=$rc" in sh
    assert 'if [ "${WP_INPUT_RC:-0}" != "0" ]; then' in sh
