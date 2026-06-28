-- =============================================================================
-- UEH Seed: Tenable Field Mappings (Tenable → slv_vulnerability_findings)
-- =============================================================================
-- Maps Tenable export payload_json fields to the canonical
-- slv_vulnerability_findings Silver table.
--
-- Tenable payload_json structure (one finding record):
-- {
--   "asset": {
--     "uuid": "abc-123",
--     "hostname": "srv-prod-01",
--     "ipv4": "10.0.1.50",
--     "fqdn": "srv-prod-01.corp.com",
--     "operating_system": ["Ubuntu 22.04 LTS"],
--     "network_id": "00000000-0000-0000-0000-000000000000"
--   },
--   "plugin": {
--     "id": 12345,
--     "name": "Apache HTTP Server < 2.4.56 Multiple Vulnerabilities",
--     "family": "Web Servers",
--     "cvss_base_score": 9.8,
--     "cve": ["CVE-2024-12345", "CVE-2024-12346"],
--     "solution": "Upgrade to Apache 2.4.56 or later.",
--     "vpr": {"score": 9.2}
--   },
--   "port": {"port": 443, "protocol": "TCP", "service": "https"},
--   "severity": 4,
--   "state": "open",
--   "first_found": "2024-03-15T10:30:00Z",
--   "last_found": "2024-06-20T08:00:00Z",
--   "output": "Installed version: 2.4.51..."
-- }
--
-- Database: t01_ueh_dev_ctl
-- Run AFTER: control tables created
-- =============================================================================

USE t01_ueh_dev_ctl;


-- ─────────────────────────────────────────────────────────────────────────────
-- Tenable → slv_vulnerability_findings mappings (18 rules)
-- ─────────────────────────────────────────────────────────────────────────────

-- 1. Source Finding ID (plugin_id → string)
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_ten_001', 'default_org', 'TENABLE', 1,
    '$.plugin.id', 'source_finding_id', 'vulnerability_findings',
    'CAST', '{"cast_to":"STRING"}', true, true,
    'ueh-admin', current_timestamp(), current_timestamp()
);

-- 2. CVE ID (first CVE from array)
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_ten_002', 'default_org', 'TENABLE', 1,
    '$.plugin.cve[0]', 'cve_id', 'vulnerability_findings',
    'DIRECT', NULL, false, true,
    'ueh-admin', current_timestamp(), current_timestamp()
);

-- 3. Vulnerability Name
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_ten_003', 'default_org', 'TENABLE', 1,
    '$.plugin.name', 'vulnerability_name', 'vulnerability_findings',
    'DIRECT', NULL, false, true,
    'ueh-admin', current_timestamp(), current_timestamp()
);

-- 4. Source Asset ID (Tenable asset UUID)
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_ten_004', 'default_org', 'TENABLE', 1,
    '$.asset.uuid', 'source_asset_id', 'vulnerability_findings',
    'DIRECT', NULL, false, true,
    'ueh-admin', current_timestamp(), current_timestamp()
);

-- 5. Asset IP
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_ten_005', 'default_org', 'TENABLE', 1,
    '$.asset.ipv4', 'asset_ip', 'vulnerability_findings',
    'DIRECT', NULL, false, true,
    'ueh-admin', current_timestamp(), current_timestamp()
);

-- 6. Asset Hostname
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_ten_006', 'default_org', 'TENABLE', 1,
    '$.asset.hostname', 'asset_hostname', 'vulnerability_findings',
    'DIRECT', NULL, false, true,
    'ueh-admin', current_timestamp(), current_timestamp()
);

-- 7. Asset FQDN
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_ten_007', 'default_org', 'TENABLE', 1,
    '$.asset.fqdn', 'asset_fqdn', 'vulnerability_findings',
    'DIRECT', NULL, false, true,
    'ueh-admin', current_timestamp(), current_timestamp()
);

-- 8. Asset OS
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_ten_008', 'default_org', 'TENABLE', 1,
    '$.asset.operating_system[0]', 'asset_os', 'vulnerability_findings',
    'DIRECT', NULL, false, true,
    'ueh-admin', current_timestamp(), current_timestamp()
);

-- 9. Severity (Tenable numeric 0-4 → standard enum)
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_ten_009', 'default_org', 'TENABLE', 1,
    '$.severity', 'severity', 'vulnerability_findings',
    'LOOKUP', '{"map":{"0":"INFORMATIONAL","1":"LOW","2":"MEDIUM","3":"HIGH","4":"CRITICAL"}}',
    false, true,
    'ueh-admin', current_timestamp(), current_timestamp()
);

-- 10. CVSS Base Score
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_ten_010', 'default_org', 'TENABLE', 1,
    '$.plugin.cvss_base_score', 'cvss_base_score', 'vulnerability_findings',
    'CAST', '{"cast_to":"DOUBLE"}', false, true,
    'ueh-admin', current_timestamp(), current_timestamp()
);

-- 11. Source Risk Score (Tenable VPR)
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_ten_011', 'default_org', 'TENABLE', 1,
    '$.plugin.vpr.score', 'source_risk_score', 'vulnerability_findings',
    'CAST', '{"cast_to":"DOUBLE"}', false, true,
    'ueh-admin', current_timestamp(), current_timestamp()
);

-- 12. Status (Tenable state → normalized enum)
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_ten_012', 'default_org', 'TENABLE', 1,
    '$.state', 'status', 'vulnerability_findings',
    'LOOKUP', '{"map":{"open":"OPEN","reopened":"REOPENED","fixed":"FIXED"}}',
    false, true,
    'ueh-admin', current_timestamp(), current_timestamp()
);

-- 13. First Seen
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_ten_013', 'default_org', 'TENABLE', 1,
    '$.first_found', 'first_seen', 'vulnerability_findings',
    'CAST', '{"cast_to":"TIMESTAMP"}', false, true,
    'ueh-admin', current_timestamp(), current_timestamp()
);

-- 14. Last Seen
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_ten_014', 'default_org', 'TENABLE', 1,
    '$.last_found', 'last_seen', 'vulnerability_findings',
    'CAST', '{"cast_to":"TIMESTAMP"}', false, true,
    'ueh-admin', current_timestamp(), current_timestamp()
);

-- 15. Output (scanner proof)
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_ten_015', 'default_org', 'TENABLE', 1,
    '$.output', 'output', 'vulnerability_findings',
    'DIRECT', NULL, false, true,
    'ueh-admin', current_timestamp(), current_timestamp()
);

-- 16. Solution
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_ten_016', 'default_org', 'TENABLE', 1,
    '$.plugin.solution', 'solution', 'vulnerability_findings',
    'DIRECT', NULL, false, true,
    'ueh-admin', current_timestamp(), current_timestamp()
);

-- 17. Port
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_ten_017', 'default_org', 'TENABLE', 1,
    '$.port.port', 'port', 'vulnerability_findings',
    'CAST', '{"cast_to":"INT"}', false, true,
    'ueh-admin', current_timestamp(), current_timestamp()
);

-- 18. Protocol
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_ten_018', 'default_org', 'TENABLE', 1,
    '$.port.protocol', 'protocol', 'vulnerability_findings',
    'UPPER', NULL, false, true,
    'ueh-admin', current_timestamp(), current_timestamp()
);


-- ─────────────────────────────────────────────────────────────────────────────
-- Verify
-- ─────────────────────────────────────────────────────────────────────────────

SELECT mapping_id, source_json_path, target_field, transformation_type
FROM t01_ueh_ctl_field_mapping
WHERE source_system = 'TENABLE' AND is_active = true
ORDER BY mapping_id;

-- Expected: 18 rows (map_ten_001 through map_ten_018)
