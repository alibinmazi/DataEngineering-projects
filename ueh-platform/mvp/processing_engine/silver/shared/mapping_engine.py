"""
=============================================================================
UEH Silver Shared: Mapping Engine
=============================================================================
Reads field_mapping control table and applies transformations to payload_json.
Used by ALL 3 Silver jobs (vulnerability_intel, vulnerability_findings, assets).

Responsibilities:
    - Read active mappings for a source_system from field_mapping table
    - Apply get_json_object to extract fields from payload_json
    - Apply transformation (DIRECT, CAST, UPPER, LOWER, TRIM, LOOKUP, TO_JSON)
    - Return DataFrame with new columns added
=============================================================================
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, get_json_object, lit, when, upper, lower, trim, to_timestamp, expr
)
from pyspark.sql.types import DoubleType, IntegerType, DateType
import json
import logging

logger = logging.getLogger("UEH-Silver-Mapping")


def read_field_mappings(spark: SparkSession, db_control: str,
                        source_system: str, org_id: str) -> list:
    """
    Read active field mappings from control table.
    
    Returns list of Row objects with:
        mapping_id, source_json_path, target_field,
        transformation_type, transformation_config, is_required
    """
    mappings = spark.sql(f"""
        SELECT mapping_id, source_json_path, target_field,
               transformation_type, transformation_config, is_required
        FROM {db_control}.t01_ueh_ctl_field_mapping
        WHERE source_system = '{source_system}'
          AND org_id = '{org_id}'
          AND is_active = TRUE
        ORDER BY mapping_id
    """).collect()

    if len(mappings) == 0:
        raise Exception(
            f"No active field mappings found for source_system='{source_system}', "
            f"org_id='{org_id}'. Configure mappings in UEH Dashboard or seed SQL."
        )

    logger.info(f"Loaded {len(mappings)} field mappings for {source_system}")
    return mappings


def apply_mappings(df: DataFrame, mappings: list) -> DataFrame:
    """
    Apply field mappings to extract and transform values from payload_json.
    
    For each mapping:
        1. get_json_object(payload_json, source_json_path) → raw value
        2. Apply transformation_type (CAST, UPPER, etc.)
        3. Add as new column with target_field name
    """
    for mapping in mappings:
        source_path = mapping.source_json_path
        target_field = mapping.target_field
        transform_type = (mapping.transformation_type or 'DIRECT').upper()
        transform_config = mapping.transformation_config

        # Extract raw value from JSON
        extracted = get_json_object(col("payload_json"), source_path)

        # Apply transformation
        transformed = _apply_transformation(extracted, transform_type, transform_config)

        # Add column
        df = df.withColumn(target_field, transformed)

    return df


def _apply_transformation(column, transform_type: str, config_json: str):
    """Apply a single transformation to an extracted column value."""

    config = {}
    if config_json:
        try:
            config = json.loads(config_json)
        except json.JSONDecodeError:
            logger.warning(f"Invalid transformation_config JSON: {config_json}")

    if transform_type == 'DIRECT':
        return column

    elif transform_type == 'CAST':
        cast_to = config.get('cast_to', 'STRING').upper()
        if cast_to == 'DOUBLE':
            return column.cast(DoubleType())
        elif cast_to in ('INT', 'INTEGER'):
            return column.cast(IntegerType())
        elif cast_to == 'TIMESTAMP':
            fmt = config.get('format')
            if fmt:
                return to_timestamp(column, fmt)
            else:
                return to_timestamp(column)
        elif cast_to == 'DATE':
            return column.cast(DateType())
        else:
            return column

    elif transform_type == 'UPPER':
        return upper(column)

    elif transform_type == 'LOWER':
        return lower(column)

    elif transform_type == 'TRIM':
        return trim(column)

    elif transform_type == 'TO_JSON':
        # get_json_object already returns JSON string for nested objects
        return column

    elif transform_type == 'LOOKUP':
        lookup_map = config.get('map', {})
        result = lit(None)
        for from_val, to_val in lookup_map.items():
            result = when(column == str(from_val), lit(to_val)).otherwise(result)
        # Fallback to original value if no match
        result = when(result.isNull(), column).otherwise(result)
        return result

    elif transform_type == 'EXPRESSION':
        expr_str = config.get('expr', '')
        if expr_str:
            return expr(expr_str)
        return column

    else:
        logger.warning(f"Unknown transformation_type: {transform_type}. Using DIRECT.")
        return column
