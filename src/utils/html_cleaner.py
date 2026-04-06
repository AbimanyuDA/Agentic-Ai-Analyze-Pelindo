"""
src/utils/html_cleaner.py - Clean HTML tags and entities from ticket descriptions

Uses fast regex-based approach (avoids BeautifulSoup per-row overhead on large datasets).
"""
from __future__ import annotations
import re
import html

# Pre-compiled patterns for speed
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")
_TIMESTAMP_RE = re.compile(
    r"\d{2}-\d{2}-\d{4}\s+\d{2}:\d{2}:\d{2}\s+-\s+[^\n]+\(Root Cause and Solution\)\s*"
)
_BR_RE = re.compile(r"<br\s*/?>", re.IGNORECASE)
_BOILERPLATE = [
    re.compile(r"Dear\s+[Tt]im.*?,\s*"),
    re.compile(r"Mohon bantuannya\s*"),
    re.compile(r"Terima kasih\s*"),
    re.compile(r"~\w+\s*$", re.MULTILINE),
]


def clean_text(raw: str | None) -> str:
    """Strip HTML tags, decode entities, normalize whitespace. Fast regex version."""
    if not raw:
        return ""
    text = str(raw)
    # Decode HTML entities (&amp; → &, &#43; → +, etc.)
    text = html.unescape(text)
    # Replace <br> tags with space before stripping all tags
    text = _BR_RE.sub(" ", text)
    # Remove all remaining HTML tags
    text = _TAG_RE.sub(" ", text)
    # Normalize whitespace
    text = _WS_RE.sub(" ", text).strip()
    # Remove common boilerplate
    for pat in _BOILERPLATE:
        text = pat.sub("", text)
    return text.strip()


def clean_resolved_notes(raw: str | None) -> str:
    """Extract the final meaningful resolution from Resolved Notes."""
    if not raw:
        return ""
    cleaned = clean_text(raw)
    # Remove timestamp prefixes like "07-09-2025 20:10:05 - IT Support HO (Root Cause and Solution)"
    cleaned = _TIMESTAMP_RE.sub("", cleaned)
    return cleaned[:600].strip()
