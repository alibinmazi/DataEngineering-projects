"""
=============================================================================
UEH Silver Shared: Cleaning Engine
=============================================================================
Standard data cleaning rules applied to all Silver outputs.
Used by ALL 3 Silver jobs.

Responsibilities:
    - Empty string → NULL
    - Trim whitespace
    - Standardize enums (severity, status → UPPER)
    - Remove known placeholders (N/A, None, null → NULL)
=============================================================================
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, trim, upper, lit, length
import logging

logger = logging.getLogger("UEH-Silver-Cleaning")

# Known placeholder values that should become NULL
NULL_PLACEHOLDERS = ['', 'N/A', 'n/a', 'NA', 'None', 'none', 'null', 'NULL', '-', '--']

# Enum fields that should always be UPPERCASE
ENUM_FIELDS = ['severity', 'status', 'asset_type', 'asset_subtype',
               'os_family', 'environment', 'criticality']


def clean_data(df: DataFrame, mapped_fields: list) -> DataFrame:
    """
    Apply standard cleaning to all mapped fields.
    
    Args:
        df: DataFrame with mapped columns
        mapped_fields: List of target_field names that were mapped
    """
    for field in mapped_fields:
        if field not in df.columns:
            continue

        # 1. Trim whitespace
        df = df.withColumn(field,
                           when(col(field).isNotNull(), trim(col(field)))
                           .otherwise(lit(None)))

        # 2. Convert placeholders to NULL
        for placeholder in NULL_PLACEHOLDERS:
            df = df.withColumn(field,
                               when(col(field) == placeholder, lit(None))
                               .otherwise(col(field)))

        # 3. Uppercase enum fields
        if field in ENUM_FIELDS:
            df = df.withColumn(field,
                               when(col(field).isNotNull(), upper(col(field)))
                               .otherwise(lit(None)))

    logger.info(f"Cleaning applied to {len(mapped_fields)} fields")
    return df
