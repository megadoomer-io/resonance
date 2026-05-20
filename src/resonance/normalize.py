"""Text normalization for entity name comparison.

Normalizes unicode, standardizes punctuation, and strips diacritics
so that names from different sources can be reliably compared. Original
display names are preserved elsewhere — this module is for comparison only.
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
