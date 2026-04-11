"""Confidence scoring for signal candidates.

Weights are initial guesses from specs/03-correlation-engine.md.
Phase 5's performance tracker will provide empirical data for tuning.
"""

# Evidence weight table -- each key maps to a confidence point value
WEIGHTS: dict[str, int] = {
    # Committee overlap (Matcher 1)
    "committee_chair": 25,
    "committee_ranking_member": 20,
    "committee_member": 15,

    # Legislation timing (Matcher 2)
    "legislation_within_7d": 20,
    "legislation_within_30d": 10,
    "legislation_sponsor_bonus": 10,
}


def score_evidence(evidence_keys: list[str]) -> float:
    """Sum weights for a list of evidence keys. Cap at 100.

    Args:
        evidence_keys: list of keys into WEIGHTS (e.g. ["committee_chair", "legislation_within_7d"])

    Returns:
        Confidence score between 0 and 100.
    """
    total = sum(WEIGHTS.get(k, 0) for k in evidence_keys)
    return min(total, 100.0)
