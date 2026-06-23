"""Source URL helpers shared by DART query and FAISS metadata paths."""

from typing import Any, Optional


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
