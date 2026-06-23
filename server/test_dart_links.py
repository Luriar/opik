import os
import sys
import types
import unittest

import pandas as pd


sys.path.insert(0, os.path.dirname(__file__))
sys.modules.setdefault("pyarrow", types.ModuleType("pyarrow"))
sys.modules.setdefault("pyarrow.parquet", types.ModuleType("pyarrow.parquet"))

from dart_query import _dart_view_url, _source_line, _source_url_for_row
from source_links import source_url_from_metadata


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


if __name__ == "__main__":
    unittest.main()
