"""Text and geographic normalization for entity comparison.

Normalizes unicode, standardizes punctuation, strips diacritics,
and expands geographic abbreviations so that names and location fields
from different sources can be reliably compared. Original display values
are preserved elsewhere — this module is for comparison only.
"""

from __future__ import annotations

import re
import unicodedata

_QUOTE_MAP = str.maketrans(
    {
        "‘": "'",  # left single quotation mark
        "’": "'",  # right single quotation mark (smart apostrophe)
        "‚": "'",  # single low-9 quotation mark
        "′": "'",  # prime
        "“": '"',  # left double quotation mark
        "”": '"',  # right double quotation mark
        "„": '"',  # double low-9 quotation mark
        "″": '"',  # double prime
    }
)

_WHITESPACE = re.compile(r"\s+")


def _strip_diacritics(value: str) -> str:
    """Remove diacritical marks (accents, umlauts, etc.) from text."""
    decomposed = unicodedata.normalize("NFD", value)
    stripped = "".join(c for c in decomposed if unicodedata.category(c) != "Mn")
    return unicodedata.normalize("NFC", stripped)


def normalize_name(value: str) -> str:
    """Normalize a name for comparison purposes.

    Steps:
        1. NFC unicode normalization (composed form)
        2. Standardize smart quotes/apostrophes to ASCII
        3. Strip diacritics (accents, umlauts, cedillas)
        4. Case fold (full unicode case folding)
        5. Collapse and strip whitespace

    Args:
        value: The name to normalize.

    Returns:
        Normalized lowercase string suitable for equality comparison.
    """
    if not value:
        return ""
    result = unicodedata.normalize("NFC", value)
    result = result.translate(_QUOTE_MAP)
    result = _strip_diacritics(result)
    result = result.casefold()
    result = _WHITESPACE.sub(" ", result).strip()
    return result


def names_match(a: str, b: str) -> bool:
    """Check whether two names refer to the same entity after normalization."""
    return normalize_name(a) == normalize_name(b)


# ---------------------------------------------------------------------------
# Geographic normalization — state/country abbreviation expansion
# ---------------------------------------------------------------------------

_US_STATES: dict[str, str] = {
    "al": "alabama",
    "ak": "alaska",
    "az": "arizona",
    "ar": "arkansas",
    "ca": "california",
    "co": "colorado",
    "ct": "connecticut",
    "de": "delaware",
    "fl": "florida",
    "ga": "georgia",
    "hi": "hawaii",
    "id": "idaho",
    "il": "illinois",
    "in": "indiana",
    "ia": "iowa",
    "ks": "kansas",
    "ky": "kentucky",
    "la": "louisiana",
    "me": "maine",
    "md": "maryland",
    "ma": "massachusetts",
    "mi": "michigan",
    "mn": "minnesota",
    "ms": "mississippi",
    "mo": "missouri",
    "mt": "montana",
    "ne": "nebraska",
    "nv": "nevada",
    "nh": "new hampshire",
    "nj": "new jersey",
    "nm": "new mexico",
    "ny": "new york",
    "nc": "north carolina",
    "nd": "north dakota",
    "oh": "ohio",
    "ok": "oklahoma",
    "or": "oregon",
    "pa": "pennsylvania",
    "ri": "rhode island",
    "sc": "south carolina",
    "sd": "south dakota",
    "tn": "tennessee",
    "tx": "texas",
    "ut": "utah",
    "vt": "vermont",
    "va": "virginia",
    "wa": "washington",
    "wv": "west virginia",
    "wi": "wisconsin",
    "wy": "wyoming",
    "dc": "district of columbia",
    "pr": "puerto rico",
    "gu": "guam",
    "vi": "virgin islands",
    "as": "american samoa",
    "mp": "northern mariana islands",
}

_COUNTRIES: dict[str, str] = {
    "us": "united states",
    "gb": "united kingdom",
    "uk": "united kingdom",
    "ca": "canada",
    "au": "australia",
    "nz": "new zealand",
    "de": "germany",
    "fr": "france",
    "es": "spain",
    "it": "italy",
    "nl": "netherlands",
    "be": "belgium",
    "at": "austria",
    "ch": "switzerland",
    "se": "sweden",
    "no": "norway",
    "dk": "denmark",
    "fi": "finland",
    "ie": "ireland",
    "pt": "portugal",
    "pl": "poland",
    "cz": "czech republic",
    "hu": "hungary",
    "ro": "romania",
    "bg": "bulgaria",
    "hr": "croatia",
    "sk": "slovakia",
    "si": "slovenia",
    "ee": "estonia",
    "lv": "latvia",
    "lt": "lithuania",
    "gr": "greece",
    "jp": "japan",
    "kr": "south korea",
    "cn": "china",
    "tw": "taiwan",
    "in": "india",
    "br": "brazil",
    "mx": "mexico",
    "ar": "argentina",
    "cl": "chile",
    "co": "colombia",
    "za": "south africa",
    "il": "israel",
    "ru": "russia",
    "tr": "turkey",
    "ua": "ukraine",
}


def normalize_state(value: str) -> str:
    """Normalize a state/province value for comparison.

    Expands US state abbreviations to full names and lowercases the result.
    Non-US or unrecognized values are lowercased and whitespace-normalized.
    """
    if not value:
        return ""
    folded = value.strip().casefold()
    return _US_STATES.get(folded, folded)


def normalize_country(value: str) -> str:
    """Normalize a country value for comparison.

    Expands ISO 3166-1 alpha-2 codes to full country names and lowercases
    the result. Unrecognized values are lowercased and whitespace-normalized.
    """
    if not value:
        return ""
    folded = value.strip().casefold()
    return _COUNTRIES.get(folded, folded)


def locations_match(state_a: str, country_a: str, state_b: str, country_b: str) -> bool:
    """Check whether two state/country pairs refer to the same location."""
    return normalize_state(state_a) == normalize_state(state_b) and normalize_country(
        country_a
    ) == normalize_country(country_b)
