"""Name normalization utilities for matching members across data sources.

Capitol Trades uses "First Last" format (e.g. "Angus King").
congress-legislators GitHub uses "First Last" (e.g. "Angus King").
congress.gov votes use "First Middle Last Suffix" (e.g. "Adam B. Schiff").

This module provides a normalize function that strips these to a
canonical form for matching: lowercase, no suffixes, no middle initials.
"""

import re


_SUFFIXES = re.compile(
    r",?\s*\b(Jr\.?|Sr\.?|III|II|IV|M\.?D\.?|Ph\.?D\.?)\b\.?", re.IGNORECASE
)
_MIDDLE_INITIALS = re.compile(r"\b[A-Z]\.\s*")
_MULTI_SPACE = re.compile(r"\s+")


def normalize_member_name(name: str) -> str:
    """Normalize a member name to a canonical form for matching.

    Handles:
    - "Last, First" -> "first last" (comma inversion)
    - Strips Jr., Sr., II, III, IV, M.D., Ph.D.
    - Strips middle initials like "B." or "F."
    - Lowercases everything
    - Collapses whitespace

    Examples:
        "Smith, Adam B. Jr." -> "adam smith"
        "Angus King" -> "angus king"
        "Adam B. Schiff" -> "adam schiff"
        "Mitch McConnell" -> "mitch mcconnell"
    """
    if not name:
        return ""

    s = name.strip()

    # Handle "Last, First [Middle]" inversion
    if "," in s:
        parts = s.split(",", 1)
        s = f"{parts[1].strip()} {parts[0].strip()}"

    # Strip suffixes
    s = _SUFFIXES.sub("", s)

    # Strip middle initials (single letter followed by period)
    s = _MIDDLE_INITIALS.sub("", s)

    # Lowercase and collapse whitespace
    s = s.lower().strip()
    s = _MULTI_SPACE.sub(" ", s)

    # Strip any trailing periods or commas
    s = s.rstrip(".,")

    return s
