"""Tests for name normalization and sector canonicalization."""

import pytest

from fathom.engine.normalization import normalize_member_name
from fathom.engine.pipeline import canonicalize_sector


# --- normalize_member_name ---


class TestNormalizeMemberName:
    def test_simple_name(self):
        assert normalize_member_name("Addison McConnell") == "addison mcconnell"

    def test_strips_middle_initial(self):
        assert normalize_member_name("Adam B. Schiff") == "adam schiff"

    def test_comma_inversion_with_middle_and_suffix(self):
        assert normalize_member_name("Smith, Adam B. Jr.") == "adam smith"

    def test_comma_inversion_with_jr_suffix(self):
        assert normalize_member_name("King, Angus S. Jr.") == "angus king"

    def test_strips_iii_suffix(self):
        assert normalize_member_name("Nancy Pelosi III") == "nancy pelosi"

    def test_strips_middle_initial_no_suffix(self):
        assert normalize_member_name("Susan M. Collins") == "susan collins"

    def test_empty_string(self):
        assert normalize_member_name("") == ""

    def test_collapses_whitespace(self):
        assert normalize_member_name("  John  Boozman  ") == "john boozman"


# --- canonicalize_sector ---


class TestCanonicalizeSector:
    def test_financial_services(self):
        assert canonicalize_sector("Financial Services") == "Finance"

    def test_basic_materials(self):
        assert canonicalize_sector("Basic Materials") == "Materials"

    def test_communication_services(self):
        assert canonicalize_sector("Communication Services") == "Telecom"

    def test_consumer_cyclical(self):
        assert canonicalize_sector("Consumer Cyclical") == "Consumer"

    def test_consumer_defensive(self):
        assert canonicalize_sector("Consumer Defensive") == "Consumer"

    def test_industrials(self):
        assert canonicalize_sector("Industrials") == "Industrial"

    def test_passthrough_technology(self):
        assert canonicalize_sector("Technology") == "Technology"

    def test_passthrough_defense(self):
        assert canonicalize_sector("Defense") == "Defense"

    def test_none_returns_none(self):
        assert canonicalize_sector(None) is None
