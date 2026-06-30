-- =============================================================================
-- UEH Seed: BMC ADDM Field Mappings (ADDM → slv_assets)
-- =============================================================================
-- Maps BMC ADDM payload_json fields to the canonical slv_assets Silver table.
--
-- ADDM payload_json structure (one host/asset record):
-- {
--   "key": "HOST-abc123def456",
--   "type": "Host",
--   "hostname": "srv-prod-01",
--   "#ip": "10.0.1.50",
--   "fqdn": "srv-prod-01.corp.internal",
--   "mac_address": "00:1A:2B:3C:4D:5E",
--   "os": "Ubuntu 22.04.3 LTS",
--   "os_class": "Linux",
--   "os_version": "22.04",
--   "domain": "corp.internal",
--   "location": "DC-US-EAST-1",
--   "last_update_success": "2024-06-19T22:30:00.000Z",
--   "first_discovered": "2023-01-15T08:00:00.000Z",
--   "#cpucount": 8,
--   "#ram": 32768,
--   "#disk_total": 500,
--   "vendor": "Dell",
--   "model": "PowerEdge R740",
--   "serial": "SN-XYZ123",
--   "virtual": true,
--   "hypervisor": "VMware ESXi 7.0",
--   "cluster": "prod-cluster-01",
--   "business_service": "E-Commerce Platform",
--   "support_group": "Infrastructure-Team-US"
-- }
--
-- Database: t01_ueh_dev_ctl
-- Run AFTER: control tables created
-- =============================================================================

USE t01_ueh_dev_ctl;


-- ─────────────────────────────────────────────────────────────────────────────
-- ADDM → slv_assets mappings (15 rules)
-- ─────────────────────────────────────────────────────────────────────────────

-- 1. Source Asset ID (ADDM internal key)
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_addm_001', 'default_org', 'BMC_ADDM', 1,
    '$.key', 'source_asset_id', 'assets',
    'DIRECT', NULL, true, true,
    'ueh-admin', current_timestamp(), current_timestamp()
);

-- 2. Asset Type (ADDM type → normalized enum)
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_addm_002', 'default_org', 'BMC_ADDM', 1,
    '$.type', 'asset_type', 'assets',
    'LOOKUP', '{"map":{"Host":"COMPUTE","NetworkDevice":"NETWORK","Printer":"OTHER","StorageDevice":"STORAGE","VirtualMachine":"COMPUTE","Container":"CONTAINER"}}',
    false, true,
    'ueh-admin', current_timestamp(), current_timestamp()
);

-- 3. Hostname
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_addm_003', 'default_org', 'BMC_ADDM', 1,
    '$.hostname', 'hostname', 'assets',
    'LOWER', NULL, false, true,
    'ueh-admin', current_timestamp(), current_timestamp()
);

-- 4. IP Address
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_addm_004', 'default_org', 'BMC_ADDM', 1,
    '$.#ip', 'ip_address', 'assets',
    'DIRECT', NULL, false, true,
    'ueh-admin', current_timestamp(), current_timestamp()
);

-- 5. FQDN
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_addm_005', 'default_org', 'BMC_ADDM', 1,
    '$.fqdn', 'fqdn', 'assets',
    'LOWER', NULL, false, true,
    'ueh-admin', current_timestamp(), current_timestamp()
);

-- 6. MAC Address
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_addm_006', 'default_org', 'BMC_ADDM', 1,
    '$.mac_address', 'mac_address', 'assets',
    'UPPER', NULL, false, true,
    'ueh-admin', current_timestamp(), current_timestamp()
);

-- 7. Operating System (full string)
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_addm_007', 'default_org', 'BMC_ADDM', 1,
    '$.os', 'operating_system', 'assets',
    'DIRECT', NULL, false, true,
    'ueh-admin', current_timestamp(), current_timestamp()
);

-- 8. OS Family (normalized enum)
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_addm_008', 'default_org', 'BMC_ADDM', 1,
    '$.os_class', 'os_family', 'assets',
    'LOOKUP', '{"map":{"Windows":"WINDOWS","Linux":"LINUX","UNIX":"LINUX","macOS":"MACOS","VMware":"LINUX","IOS":"NETWORK_OS","NX-OS":"NETWORK_OS","JunOS":"NETWORK_OS"}}',
    false, true,
    'ueh-admin', current_timestamp(), current_timestamp()
);

-- 9. Location
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_addm_009', 'default_org', 'BMC_ADDM', 1,
    '$.location', 'location', 'assets',
    'DIRECT', NULL, false, true,
    'ueh-admin', current_timestamp(), current_timestamp()
);

-- 10. Business Unit (from domain or business_service)
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_addm_010', 'default_org', 'BMC_ADDM', 1,
    '$.business_service', 'business_unit', 'assets',
    'DIRECT', NULL, false, true,
    'ueh-admin', current_timestamp(), current_timestamp()
);

-- 11. Owner (support group as owner)
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_addm_011', 'default_org', 'BMC_ADDM', 1,
    '$.support_group', 'owner', 'assets',
    'DIRECT', NULL, false, true,
    'ueh-admin', current_timestamp(), current_timestamp()
);

-- 12. Last Seen (last successful discovery)
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_addm_012', 'default_org', 'BMC_ADDM', 1,
    '$.last_update_success', 'last_seen', 'assets',
    'CAST', '{"cast_to":"TIMESTAMP"}', false, true,
    'ueh-admin', current_timestamp(), current_timestamp()
);

-- 13. First Seen (first discovered)
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_addm_013', 'default_org', 'BMC_ADDM', 1,
    '$.first_discovered', 'first_seen', 'assets',
    'CAST', '{"cast_to":"TIMESTAMP"}', false, true,
    'ueh-admin', current_timestamp(), current_timestamp()
);

-- 14. Asset Subtype (from virtual flag + model)
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_addm_014', 'default_org', 'BMC_ADDM', 1,
    '$.model', 'asset_subtype', 'assets',
    'DIRECT', NULL, false, true,
    'ueh-admin', current_timestamp(), current_timestamp()
);

-- 15. Asset Attributes JSON (hardware details — category-specific extras)
INSERT INTO t01_ueh_ctl_field_mapping VALUES (
    'map_addm_015', 'default_org', 'BMC_ADDM', 1,
    '$', 'asset_attributes_json', 'assets',
    'TO_JSON', NULL, false, true,
    'ueh-admin', current_timestamp(), current_timestamp()
);


-- ─────────────────────────────────────────────────────────────────────────────
-- Verify
-- ─────────────────────────────────────────────────────────────────────────────

SELECT mapping_id, source_json_path, target_field, transformation_type
FROM t01_ueh_ctl_field_mapping
WHERE source_system = 'BMC_ADDM' AND is_active = true
ORDER BY mapping_id;

-- Expected: 15 rows (map_addm_001 through map_addm_015)


-- =============================================================================
-- VERIFY ALL FIELD MAPPINGS ACROSS ALL SOURCES:
-- =============================================================================

SELECT source_system, COUNT(*) as mapping_count
FROM t01_ueh_ctl_field_mapping
WHERE is_active = true
GROUP BY source_system
ORDER BY source_system;

-- Expected:
-- BMC_ADDM  | 15
-- NVD       | 6  (or 11 if full seed ran)
-- TENABLE   | 18
