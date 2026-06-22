# Change Log

## 2026-06-23

- Fixed chatbot DART intent routing.
  - Added deterministic DART intent override in `server/intent_parser.py` so explicit DART/disclosure/financial/shareholder/insider keywords do not stay misclassified as `report_search`.
  - Applied the same override in `/v2/chat` agent routing via `server/agent_integration.py`.
  - Routed `/v2/chat` DART sub-intents to the matching `DartAgent` query wrapper instead of always querying disclosure events.
  - Added `server/test_intent_override.py` to verify the override without Bedrock or S3.
