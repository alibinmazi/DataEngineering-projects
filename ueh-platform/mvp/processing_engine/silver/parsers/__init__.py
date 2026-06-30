# UEH Silver Parsers - Adapter-specific parsing logic
# Each parser is responsible for:
#   - Nested JSON extraction from payload_json
#   - API-specific structure handling
#   - Complex transformation logic
#   - DQ flag computation for Stage 1
#
# Parser classes are VERSIONED (e.g., nvd_parser_v1)
# When API schema changes, create v2 rather than modifying v1
