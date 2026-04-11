"""Tests for the evidence scoring module."""

import pytest

from fathom.engine.scorer import WEIGHTS, score_evidence


class TestScoreEvidence:
    def test_committee_chair(self):
        assert score_evidence(["committee_chair"]) == 25.0

    def test_committee_member(self):
        assert score_evidence(["committee_member"]) == 15.0

    def test_committee_ranking_member(self):
        assert score_evidence(["committee_ranking_member"]) == 20.0

    def test_legislation_within_7d(self):
        assert score_evidence(["legislation_within_7d"]) == 20.0

    def test_legislation_within_30d(self):
        assert score_evidence(["legislation_within_30d"]) == 10.0

    def test_stacking_two_keys(self):
        assert score_evidence(["committee_chair", "legislation_within_7d"]) == 45.0

    def test_stacking_three_keys(self):
        result = score_evidence(
            ["committee_chair", "legislation_within_7d", "legislation_sponsor_bonus"]
        )
        assert result == 55.0

    def test_all_keys_capped_at_100(self):
        all_keys = list(WEIGHTS.keys())
        raw_total = sum(WEIGHTS.values())
        assert raw_total == 100  # sanity check: weights sum to exactly 100
        assert score_evidence(all_keys) == 100.0

    def test_empty_list(self):
        assert score_evidence([]) == 0.0

    def test_unknown_key(self):
        assert score_evidence(["nonexistent_key"]) == 0.0
