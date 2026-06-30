-- =============================================================================
-- UEH Seed: NVD Field Mappings (NVD → slv_vulnerability_intel)
-- =============================================================================
-- Purpose: Defines how to parse NVD payload_json into Silver canonical fields.
--          This is what the analyst would configure via UI for NVD.
--          For MVP, we seed it directly via SQL.
--
-- These mappings tell the generic Silver transformer:
--   "For NVD source, extract THIS json path → put into THIS Silver column"
--
-- Run AFTER: infrastructure/control_tables/04_field_mapping.sql
-- Database: t01_ueh_dev_ctl
--
-- NVD payload_json structure (each record in Bronze):
-- {
--   "cve": {
--     "id": "CVE-2024-12345",
--     "sourceIdentifier": "cve@mitre.org",
--     "published": "2024-01-15T10:15:00.000",
--     "lastModified": "2024-06-01T12:00:00.000",
--     "descriptions": [
--       {"lang": "en", "value": "A vulnerability in..."}
--     ],
--     "metrics": {
--       "cvssMetricV31": [
--         {
--           "source": "nvd@nist.gov",
--           "cvssData": {
--             "version": "3.1",
--             "baseScore": 9.8,
--             "baseSeverity": "CRITICAL"
--           }
--         }
--       ]
--     },
--     "weaknesses": [
--       {"description": [{"lang": "en", "value": "CWE-79"}]}
--     ],
--     "references": [
--       {"url": "https://...", "source": "..."}
--     ]
--   }
-- }
-- =============================================================================

USE t01_ueh_dev_ctl;


-- ─────────────────────────────────────────────────────────────────────────────
-- NVD → slv_vulnerability_intel mappings
-- ─────────────────────────────────────────────────────────────────────────────

-- Core Identification
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_nvd_001', 'default_org', 'NVD', 1,
    '$.cve.id',
    'cve_id',
    'vulnerability_intel',
    'DIRECT', NULL,
    TRUE, TRUE,
    'ueh-admin', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- CVSS Base Score
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_nvd_002', 'default_org', 'NVD', 1,
    '$.cve.metrics.cvssMetricV31[0].cvssData.baseScore',
    'cvss_base_score',
    'vulnerability_intel',
    'CAST', '{"cast_to": "DOUBLE"}',
    FALSE, TRUE,
    'ueh-admin', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- CVSS Version
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_nvd_003', 'default_org', 'NVD', 1,
    '$.cve.metrics.cvssMetricV31[0].cvssData.version',
    'cvss_version',
    'vulnerability_intel',
    'DIRECT', NULL,
    FALSE, TRUE,
    'ueh-admin', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- Severity (baseSeverity from CVSS)
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_nvd_004', 'default_org', 'NVD', 1,
    '$.cve.metrics.cvssMetricV31[0].cvssData.baseSeverity',
    'severity',
    'vulnerability_intel',
    'UPPER', NULL,
    FALSE, TRUE,
    'ueh-admin', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- Description (English)
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_nvd_005', 'default_org', 'NVD', 1,
    '$.cve.descriptions[0].value',
    'description',
    'vulnerability_intel',
    'DIRECT', NULL,
    FALSE, TRUE,
    'ueh-admin', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- Published Date
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_nvd_006', 'default_org', 'NVD', 1,
    '$.cve.published',
    'published_date',
    'vulnerability_intel',
    'CAST', '{"cast_to": "TIMESTAMP", "format": "yyyy-MM-dd''T''HH:mm:ss.SSS"}',
    FALSE, TRUE,
    'ueh-admin', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- Last Modified Date
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_nvd_007', 'default_org', 'NVD', 1,
    '$.cve.lastModified',
    'last_modified_date',
    'vulnerability_intel',
    'CAST', '{"cast_to": "TIMESTAMP", "format": "yyyy-MM-dd''T''HH:mm:ss.SSS"}',
    FALSE, TRUE,
    'ueh-admin', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- Source Identifier
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_nvd_008', 'default_org', 'NVD', 1,
    '$.cve.sourceIdentifier',
    'source_identifier',
    'vulnerability_intel',
    'DIRECT', NULL,
    FALSE, TRUE,
    'ueh-admin', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- References (full array as JSON string)
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_nvd_009', 'default_org', 'NVD', 1,
    '$.cve.references',
    'references_json',
    'vulnerability_intel',
    'TO_JSON', NULL,
    FALSE, TRUE,
    'ueh-admin', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- Weaknesses / CWE (array as JSON string)
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_nvd_010', 'default_org', 'NVD', 1,
    '$.cve.weaknesses',
    'weaknesses_json',
    'vulnerability_intel',
    'TO_JSON', NULL,
    FALSE, TRUE,
    'ueh-admin', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- Affected Products / Configurations (as JSON)
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_nvd_011', 'default_org', 'NVD', 1,
    '$.cve.configurations',
    'affected_products_json',
    'vulnerability_intel',
    'TO_JSON', NULL,
    FALSE, TRUE,
    'ueh-admin', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);


-- ─────────────────────────────────────────────────────────────────────────────
-- Verification
-- ─────────────────────────────────────────────────────────────────────────────

SELECT mapping_id, source_json_path, target_field, transformation_type, is_required
FROM t01_ueh_ctl_field_mapping
WHERE source_system = 'NVD'
  AND is_active = TRUE
ORDER BY mapping_id;

-- Expected: 11 rows (map_nvd_001 through map_nvd_011)


-- =============================================================================
-- NOTES FOR SILVER TRANSFORMER:
-- =============================================================================
--
-- The generic_silver_transformer.py will:
--   1. Read these mappings WHERE source_system = 'NVD' AND is_active = TRUE
--   2. For each mapping row:
--      - Extract value: get_json_object(payload_json, source_json_path)
--      - Apply transformation_type:
--          DIRECT → use as-is
--          CAST   → cast to type specified in transformation_config
--          UPPER  → uppercase the value
--          TO_JSON → keep nested structure as JSON string
--      - Place result in target_field column
--   3. Write to slv_vulnerability_intel table
--
-- TRANSFORMATION TYPES SUPPORTED:
--   DIRECT     → No transformation, use raw extracted value
--   CAST       → Cast to type: {"cast_to": "DOUBLE|TIMESTAMP|INT|DATE", "format": "..."}
--   UPPER      → Convert to uppercase (for enum standardization)
--   LOWER      → Convert to lowercase
--   TRIM       → Trim whitespace
--   TO_JSON    → Convert nested object/array to JSON string
--   LOOKUP     → Map value using lookup: {"map": {"1":"LOW","2":"MEDIUM",...}}
--   EXPRESSION → Spark SQL expression: {"expr": "CASE WHEN ... THEN ... END"}
--   CUSTOM     → Custom UDF logic (future)
--
-- =============================================================================
