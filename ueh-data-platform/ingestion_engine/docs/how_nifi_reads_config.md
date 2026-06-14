# How NiFi Reads Configuration (Generic Flow)

## Principle

NiFi does NOT have hardcoded adapter logic. ONE generic flow handles all REST API adapters.

## Flow Logic

```
1. NiFi receives trigger (Airflow or self-scheduled)
2. Reads adapter_instance_id from trigger message (or iterates active adapters)
3. Queries adapter_config → gets: base_url, auth_method, pagination_config_json
4. Queries adapter_state → gets: watermark_state_json
5. Calls API using config-driven parameters
6. Paginates using pagination_config_json rules
7. Writes chunks to HDFS at resolved path_template
8. Updates batch_registry → RAW_COMPLETE
9. Updates adapter_state → new watermark
```

## What NiFi Reads from Control Tables

| Table | Fields Used | Purpose |
|-------|-------------|---------|
| adapter_config | base_url, auth_method, auth_secret_ref, pagination_config_json, runtime_config_json, path_template | How to call API |
| adapter_state | watermark_state_json | Where to resume |
| batch_registry | (writes to) | Register completion |

## What NiFi Does NOT Know

- Source-specific parsing logic (that's Silver)
- Field mappings (that's Silver)
- Business rules (that's Gold)
- How to handle 27 different APIs differently (it doesn't — it reads config)

## Parameterization Points

All these come from `pagination_config_json`:
- Page size parameter name
- Offset/cursor parameter name
- Total results JSONPath
- Whether pagination is offset-based or cursor-based

All these come from `runtime_config_json`:
- Timeout
- Max retries
- Rate limit
- Backoff strategy
