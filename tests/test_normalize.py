"""Tests for text normalization utility."""

from __future__ import annotations

import resonance.normalize as normalize_module


class TestNormalizeName:
    def test_ascii_passthrough(self) -> None:
        assert normalize_module.normalize_name("Iron Maiden") == "iron maiden"

    def test_casefold(self) -> None:
        assert normalize_module.normalize_name("IRON MAIDEN") == "iron maiden"

    def test_smart_single_quotes(self) -> None:
        result = normalize_module.normalize_name("KK’s Priest")
        assert result == "kk's priest"

    def test_left_single_quote(self) -> None:
        result = normalize_module.normalize_name("‘quoted’")
        assert result == "'quoted'"

    def test_smart_double_quotes(self) -> None:
        result = normalize_module.normalize_name("“Hello”")
        assert result == '"hello"'

    def test_prime_marks(self) -> None:
        result = normalize_module.normalize_name("5′ 10″")
        assert result == "5' 10\""

    def test_diacritics_stripped(self) -> None:
        assert normalize_module.normalize_name("Beyoncé") == "beyonce"

    def test_umlaut_stripped(self) -> None:
        assert normalize_module.normalize_name("Motörhead") == "motorhead"

    def test_nfc_vs_nfd_match(self) -> None:
        nfc = "Beyoncé"  # single codepoint
        nfd = "Beyoncé"  # base + combining acute
        assert normalize_module.normalize_name(nfc) == normalize_module.normalize_name(
            nfd
        )

    def test_whitespace_collapsed(self) -> None:
        assert normalize_module.normalize_name("  The   Band  ") == "the band"

    def test_tabs_and_newlines(self) -> None:
        assert normalize_module.normalize_name("The\t\nBand") == "the band"

    def test_empty_string(self) -> None:
        assert normalize_module.normalize_name("") == ""

    def test_whitespace_only(self) -> None:
        assert normalize_module.normalize_name("   ") == ""

    def test_mixed_normalization(self) -> None:
        result = normalize_module.normalize_name("KK’s Priest at Mötley Crüe Venue")
        assert result == "kk's priest at motley crue venue"

    def test_multiple_diacritics(self) -> None:
        assert normalize_module.normalize_name("José González") == ("jose gonzalez")

    def test_cedilla(self) -> None:
        assert normalize_module.normalize_name("François") == "francois"

    def test_tilde(self) -> None:
        assert normalize_module.normalize_name("España") == "espana"


class TestNamesMatch:
    def test_identical(self) -> None:
        assert normalize_module.names_match("Iron Maiden", "Iron Maiden")

    def test_case_difference(self) -> None:
        assert normalize_module.names_match("Iron Maiden", "iron maiden")

    def test_smart_quote_vs_ascii(self) -> None:
        assert normalize_module.names_match("KK’s Priest", "KK's Priest")

    def test_diacritic_vs_plain(self) -> None:
        assert normalize_module.names_match("Motörhead", "Motorhead")

    def test_different_names(self) -> None:
        assert not normalize_module.names_match("Iron Maiden", "Judas Priest")

    def test_whitespace_normalization(self) -> None:
        assert normalize_module.names_match("The  Band", "The Band")

    def test_empty_strings(self) -> None:
        assert normalize_module.names_match("", "")

    def test_empty_vs_nonempty(self) -> None:
        assert not normalize_module.names_match("", "Iron Maiden")


class TestNormalizeState:
    def test_abbreviation_expands(self) -> None:
        assert normalize_module.normalize_state("CA") == "california"

    def test_full_name_lowered(self) -> None:
        assert normalize_module.normalize_state("California") == "california"

    def test_case_insensitive(self) -> None:
        assert normalize_module.normalize_state("ca") == "california"
        assert normalize_module.normalize_state("Ca") == "california"

    def test_all_50_states(self) -> None:
        expected = {
            "AL": "alabama",
            "AK": "alaska",
            "AZ": "arizona",
            "AR": "arkansas",
            "CA": "california",
            "CO": "colorado",
            "CT": "connecticut",
            "DE": "delaware",
            "FL": "florida",
            "GA": "georgia",
            "HI": "hawaii",
            "ID": "idaho",
            "IL": "illinois",
            "IN": "indiana",
            "IA": "iowa",
            "KS": "kansas",
            "KY": "kentucky",
            "LA": "louisiana",
            "ME": "maine",
            "MD": "maryland",
            "MA": "massachusetts",
            "MI": "michigan",
            "MN": "minnesota",
            "MS": "mississippi",
            "MO": "missouri",
            "MT": "montana",
            "NE": "nebraska",
            "NV": "nevada",
            "NH": "new hampshire",
            "NJ": "new jersey",
            "NM": "new mexico",
            "NY": "new york",
            "NC": "north carolina",
            "ND": "north dakota",
            "OH": "ohio",
            "OK": "oklahoma",
            "OR": "oregon",
            "PA": "pennsylvania",
            "RI": "rhode island",
            "SC": "south carolina",
            "SD": "south dakota",
            "TN": "tennessee",
            "TX": "texas",
            "UT": "utah",
            "VT": "vermont",
            "VA": "virginia",
            "WA": "washington",
            "WV": "west virginia",
            "WI": "wisconsin",
            "WY": "wyoming",
        }
        for abbr, full in expected.items():
            assert normalize_module.normalize_state(abbr) == full

    def test_dc_and_territories(self) -> None:
        assert normalize_module.normalize_state("DC") == "district of columbia"
        assert normalize_module.normalize_state("PR") == "puerto rico"
        assert normalize_module.normalize_state("GU") == "guam"

    def test_whitespace_stripped(self) -> None:
        assert normalize_module.normalize_state("  CA  ") == "california"

    def test_unknown_passthrough(self) -> None:
        assert normalize_module.normalize_state("Ontario") == "ontario"
        assert normalize_module.normalize_state("Bavaria") == "bavaria"

    def test_empty(self) -> None:
        assert normalize_module.normalize_state("") == ""


class TestNormalizeCountry:
    def test_code_expands(self) -> None:
        assert normalize_module.normalize_country("US") == "united states"

    def test_full_name_lowered(self) -> None:
        assert normalize_module.normalize_country("United States") == "united states"

    def test_case_insensitive(self) -> None:
        assert normalize_module.normalize_country("us") == "united states"
        assert normalize_module.normalize_country("Us") == "united states"

    def test_uk_alias(self) -> None:
        assert normalize_module.normalize_country("UK") == "united kingdom"
        assert normalize_module.normalize_country("GB") == "united kingdom"

    def test_common_countries(self) -> None:
        assert normalize_module.normalize_country("DE") == "germany"
        assert normalize_module.normalize_country("FR") == "france"
        assert normalize_module.normalize_country("JP") == "japan"
        assert normalize_module.normalize_country("AU") == "australia"
        assert normalize_module.normalize_country("BR") == "brazil"
        assert normalize_module.normalize_country("MX") == "mexico"

    def test_whitespace_stripped(self) -> None:
        assert normalize_module.normalize_country("  US  ") == "united states"

    def test_unknown_passthrough(self) -> None:
        assert normalize_module.normalize_country("Wakanda") == "wakanda"

    def test_empty(self) -> None:
        assert normalize_module.normalize_country("") == ""


class TestLocationsMatch:
    def test_abbreviation_vs_full(self) -> None:
        assert normalize_module.locations_match(
            "CA", "US", "California", "United States"
        )

    def test_same_abbreviations(self) -> None:
        assert normalize_module.locations_match("CA", "US", "CA", "US")

    def test_same_full_names(self) -> None:
        assert normalize_module.locations_match(
            "California", "United States", "California", "United States"
        )

    def test_different_states(self) -> None:
        assert not normalize_module.locations_match("CA", "US", "NY", "US")

    def test_different_countries(self) -> None:
        assert not normalize_module.locations_match("", "US", "", "GB")

    def test_empty_values(self) -> None:
        assert normalize_module.locations_match("", "", "", "")
