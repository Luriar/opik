import os
import sys
import unittest


sys.path.insert(0, os.path.dirname(__file__))

from intent_parser import IntentResult, apply_dart_intent_override
from agent_integration import _apply_dart_override_to_agent_intent


class DartIntentOverrideTest(unittest.TestCase):
    def test_disclosure_question_overrides_report_search(self):
        result = IntentResult({
            "intent": "report_search",
            "search_query": "삼성전자 공시 뭐 있어?",
        })

        fixed = apply_dart_intent_override("삼성전자 공시 뭐 있어?", result)

        self.assertEqual(fixed.intent, "dart_disclosure")
        self.assertTrue(fixed.needs_dart)
        self.assertFalse(fixed.needs_reports)
        self.assertIsNone(fixed.search_query)

    def test_report_and_disclosure_question_becomes_hybrid(self):
        result = IntentResult({
            "intent": "report_search",
            "search_query": "삼성전자 공시와 목표주가 같이 알려줘",
        })

        fixed = apply_dart_intent_override("삼성전자 공시와 목표주가 같이 알려줘", result)

        self.assertEqual(fixed.intent, "hybrid")
        self.assertTrue(fixed.needs_dart)
        self.assertTrue(fixed.needs_reports)

    def test_financial_question_overrides_report_search(self):
        result = IntentResult({
            "intent": "report_search",
            "search_query": "삼성전자 PER PBR 알려줘",
        })

        fixed = apply_dart_intent_override("삼성전자 PER PBR 알려줘", result)

        self.assertEqual(fixed.intent, "dart_financial")

    def test_regular_report_is_disclosure_not_hybrid(self):
        result = IntentResult({
            "intent": "report_search",
            "search_query": "삼성전자 사업보고서 보여줘",
        })

        fixed = apply_dart_intent_override("삼성전자 사업보고서 보여줘", result)

        self.assertEqual(fixed.intent, "dart_disclosure")

    def test_refusal_is_not_overridden(self):
        result = IntentResult({"intent": "refuse"})

        fixed = apply_dart_intent_override("공시 보고 지금 사도 될까?", result)

        self.assertEqual(fixed.intent, "refuse")

    def test_v2_agent_override_uses_same_mapping(self):
        intent, params = _apply_dart_override_to_agent_intent(
            "삼성전자 공시 뭐 있어?",
            "report_search",
            {"ticker_names": ["삼성전자"]},
        )

        self.assertEqual(intent, "dart_disclosure")
        self.assertEqual(params["ticker_names"], ["삼성전자"])


if __name__ == "__main__":
    unittest.main()
