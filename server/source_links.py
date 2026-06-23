"""Source URL helpers shared by DART query and FAISS metadata paths."""

import re
from typing import Any, List, Optional, Tuple


EMPTY_TEXT = {"", "nan", "none", "null", "-"}


def is_empty_value(value: Any) -> bool:
    if value is None:
        return True
    try:
        # Works for pandas/numpy scalars without importing pandas here.
        if value != value:
            return True
    except Exception:
        pass
    text = str(value).strip()
    return text.lower() in EMPTY_TEXT


def dart_view_url(rcept_no: Any) -> Optional[str]:
    """Build the DART filing viewer URL from a receipt number."""
    if is_empty_value(rcept_no):
        return None
    return f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={str(rcept_no).strip()}"


def first_non_empty(row: Any, *fields: str) -> Optional[str]:
    for field in fields:
        try:
            value = row.get(field)
        except AttributeError:
            value = row[field] if field in row else None
        except Exception:
            value = None
        if not is_empty_value(value):
            return str(value).strip()
    return None


def source_url_from_metadata(row: Any) -> Optional[str]:
    """Return a preserved source URL or reconstruct the DART viewer URL."""
    return first_non_empty(
        row,
        "dart_view_url",
        "outer_dart_view_url",
        "source_url",
        "source_uri",
        "url",
    ) or dart_view_url(first_non_empty(row, "rcept_no"))


def source_line(row: Any, indent: str = "  ") -> str:
    url = source_url_from_metadata(row)
    return f"\n{indent}DART URL: {url}" if url else ""


# ---------------------------------------------------------------------------
# Answer grounding — strip fabricated DART URLs from generated answers.
#
# DART assigns receipt numbers (rcpNo) sequentially per filing date across ALL
# companies, so a plausible-looking but invented rcpNo can resolve to a totally
# unrelated company on dart.fss.or.kr (e.g. a 현대차증권 answer linking to a
# 미래에셋증권 filing). The LLM must only echo URLs whose receipt number was
# actually present in the data context; any other DART URL is a hallucination.
# ---------------------------------------------------------------------------

_RCPNO_IN_URL_RE = re.compile(r"rcpNo=(\d+)")
_RCPNO_TOKEN_RE = re.compile(r"(?<!\d)(\d{14})(?!\d)")
_LABELED_DART_URL_RE = re.compile(
    r"\n?[ \t]*(?:[-*•]\s*|\d+[.)]\s*)?(?:DART URL|원문)\s*[:：]\s*"
    r"https?://dart\.fss\.or\.kr/\S*?rcpNo=(\d+)\S*"
)
_BARE_DART_URL_RE = re.compile(r"https?://dart\.fss\.or\.kr/\S*?rcpNo=(\d+)\S*")


def grounded_rcept_numbers(context: str) -> set:
    """Receipt numbers the model may legitimately cite — those present in the data
    context (inside ``rcpNo=`` URLs or as bare 14-digit 공시번호 tokens)."""
    if not context:
        return set()
    nums = set(_RCPNO_IN_URL_RE.findall(context))
    nums |= set(_RCPNO_TOKEN_RE.findall(context))
    return nums


def strip_ungrounded_dart_urls(answer: str, context: str) -> Tuple[str, List[str]]:
    """Remove DART viewer URLs from ``answer`` whose receipt number is not present
    in ``context``. Returns ``(clean_answer, removed_rcept_numbers)``.

    Receipt numbers that ARE in the context are left untouched, so any URL the
    model faithfully reproduces from the provided data survives; only invented
    ones (which would deep-link to an unrelated company's filing) are dropped.
    """
    if not answer:
        return answer, []
    allowed = grounded_rcept_numbers(context)
    removed: List[str] = []

    def _scrub(match: "re.Match") -> str:
        rcpno = match.group(1)
        if rcpno in allowed:
            return match.group(0)
        removed.append(rcpno)
        return ""

    answer = _LABELED_DART_URL_RE.sub(_scrub, answer)
    answer = _BARE_DART_URL_RE.sub(_scrub, answer)
    return answer, removed
