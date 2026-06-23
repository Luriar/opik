import os
import sys
import types
import unittest

import pandas as pd


sys.path.insert(0, os.path.dirname(__file__))
sys.modules.setdefault("pyarrow", types.ModuleType("pyarrow"))
sys.modules.setdefault("pyarrow.parquet", types.ModuleType("pyarrow.parquet"))

from dart_query import _dart_view_url, _source_line, _source_url_for_row
from source_links import source_url_from_metadata, strip_ungrounded_dart_urls


class DartSourceLinkTest(unittest.TestCase):
    def test_prefers_gold_dart_view_url(self):
        row = pd.Series({
            "rcept_no": "20260101000001",
            "dart_view_url": "https://dart.fss.or.kr/dsaf001/main.do?rcpNo=gold-url",
        })

        self.assertEqual(
            _source_url_for_row(row),
            "https://dart.fss.or.kr/dsaf001/main.do?rcpNo=gold-url",
        )

    def test_builds_view_url_from_rcept_no(self):
        self.assertEqual(
            _dart_view_url("20260101000001"),
            "https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260101000001",
        )

    def test_source_line_uses_rcept_no_fallback(self):
        row = pd.Series({"rcept_no": "20260101000001"})

        self.assertEqual(
            _source_line(row),
            "\n  DART URL: https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260101000001",
        )

    def test_embedding_metadata_prefers_preserved_url(self):
        metadata = {
            "rcept_no": "20260101000001",
            "source_url": "https://dart.fss.or.kr/dsaf001/main.do?rcpNo=from-gold",
        }

        self.assertEqual(
            source_url_from_metadata(metadata),
            "https://dart.fss.or.kr/dsaf001/main.do?rcpNo=from-gold",
        )


class StripUngroundedDartUrlTest(unittest.TestCase):
    # Real regression: a 현대차증권 query whose context only holds 현대차증권 filings,
    # but the LLM fabricated a 임원·주요주주 entry with an invented rcpNo that
    # resolves to a 미래에셋증권 filing on the live DART site.
    CONTEXT = (
        "[20260430] 현대차증권 | 증권발행실적보고서\n"
        "  DART URL: https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260430000131\n"
        "공시번호: 20260430000815"
    )

    def test_removes_fabricated_url_keeps_grounded(self):
        answer = (
            "1. [20260430] 현대차증권 | 임원ㆍ주요주주특정증권등소유상황보고서\n"
            "   - DART URL: https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260430000313\n"
            "2. [20260430] 현대차증권 | 증권발행실적보고서\n"
            "   - DART URL: https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260430000131"
        )
        clean, removed = strip_ungrounded_dart_urls(answer, self.CONTEXT)

        self.assertEqual(removed, ["20260430000313"])
        self.assertNotIn("rcpNo=20260430000313", clean)
        self.assertIn("rcpNo=20260430000131", clean)

    def test_grounded_only_answer_is_untouched(self):
        answer = "참고: https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260430000131"
        clean, removed = strip_ungrounded_dart_urls(answer, self.CONTEXT)

        self.assertEqual(clean, answer)
        self.assertEqual(removed, [])

    def test_empty_context_strips_any_dart_url(self):
        answer = "보세요 https://dart.fss.or.kr/dsaf001/main.do?rcpNo=99999999999999"
        clean, removed = strip_ungrounded_dart_urls(answer, "")

        self.assertEqual(removed, ["99999999999999"])
        self.assertNotIn("rcpNo=", clean)


if __name__ == "__main__":
    unittest.main()
