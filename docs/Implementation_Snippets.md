# Project Implementation Snippets

This document collects concise code excerpts from the repository that implement each requested task. Each section includes the relevant file paths and minimal context for clarity.

**Task Map:**
| Task | Number | Files |
|------|--------|-------|
| Demo: Read/Write CSVs | T0007 | scripts/Extract.py, scripts/Load.py |
| Cleaning Utilities (trim, fillna, typecast) | T0008 | scripts/utils/validation_utils.py, scripts/cleaning_utils.py |
| Handle Incorrect Data Types | T0009 | scripts/utils/validation_utils.py |
| Duplicate Data Detection & Removal | T0010 | dags/etl_*.py (drop_duplicates) |
| Missing Data Handling (mean, drop) | T0011 | dags/etl_*.py (fillna strategies) |
| Config-Driven Cleaning Rules | T0012 | config/amazon_cleaning_rules.yaml |
| Aggregations (groupBy, sum, min, max) | T0013 | scripts/utils/aggregation_utils.py |
| Normalization & Scaling | T0014 | scripts/utils/normalization_utils.py |
| Feature Engineering Logic | T0015 | scripts/utils/feature_engineering_utils.py |
| Date/Time Transformations | T0016 | scripts/utils/datetime_utils.py |
| Config-Based Transformation Rules | T0017 | scripts/utils/transformation_orchestrator.py |
| Bulk Load Operations | T0018 | scripts/utils/bulk_loader.py |
| Incremental vs Full Loads | T0019 | scripts/utils/load_strategy.py |
| Handling Constraint Violations | T0020 | scripts/utils/constraint_handler.py |
| Upsert Logic | T0021 | scripts/utils/upsert_handler.py |
| Error Table Creation (Rejects) | T0022 | scripts/utils/rejected_records_handler.py |
| Build Master DAG to Trigger All Pipelines | T0023 | dags/etl_master_orchestrator.py |
| Event-Driven DAG Triggering | T0024 | dags/etl_sales.py, dags/etl_reports.py (ExternalTaskSensor) |
| Multi-DAG Dependency Management | T0025 | dags/etl_reports.py (5 ExternalTaskSensors) |
| Backfill & Catchup Features | T0026 | dags/dag_base.py (catchup, max_active_runs) |
| DAG Failure Handling Strategy | T0027 | dags/dag_base.py (retries, callbacks) |
| Combined E-T-L Pipeline | T0028 | dags/etl_customers.py, etl_products.py, etc. |
| Multi-Source Data Pipelines | T0029 | dags/etl_customers.py, etl_products.py, etl_stores.py, etl_sales.py, etl_exchange_rates.py |
| Reusable Pipeline Config | T0030 | dags/dag_base.py |
| Pipeline Execution Summary | T0031 | dags/etl_reports.py (9 reports), etl_master_orchestrator.py (JSON summary) |
| Error Recovery Workflow | T0032 | dags/dag_base.py (retries, failure callbacks) |

---

## T0007) Demo: Read/Write CSVs

**Extract: Read CSV files into DataFrames**

```python
# T0007: Implement demo script to read/write CSVs
# File: scripts/Extract.py
"""
Extract.py - Data Extraction Module
Extracts data from CSV source files and prepares for transformation.

Source Files (data/raw/dataset/):
- Customers.csv, Sales.csv, Products.csv, Stores.csv, Exchange_Rates.csv
"""
import pandas as pd
from pathlib import Path

class DataExtractor:
    """TEAM 1 - Data Extraction Handler"""
    
    # Source file definitions with expected columns
    SOURCE_FILES = {
        'customers': {'filename': 'Customers.csv', 'key_column': 'CustomerKey'},
        'sales': {'filename': 'Sales.csv', 'key_column': 'Order Number'},
        'products': {'filename': 'Products.csv', 'key_column': 'ProductKey'},
        'stores': {'filename': 'Stores.csv', 'key_column': 'StoreKey'},
        'exchange_rates': {'filename': 'Exchange_Rates.csv', 'key_column': 'Date'},
    }
    
    def __init__(self, data_dir: Path = None):
        self.data_dir = data_dir or Path("data/raw/dataset")
        self.staging_dir = Path("data/staging")
        self.staging_dir.mkdir(parents=True, exist_ok=True)
    
    def extract_table(self, table_name: str) -> pd.DataFrame:
        """
        Extract a single table from CSV
        
        Args:
            table_name: Name of table to extract (customers, sales, etc.)
        Returns:
            DataFrame with extracted data
        """
        config = self.SOURCE_FILES[table_name]
        file_path = self.data_dir / config['filename']
        
        df = pd.read_csv(file_path)
        print(f"✅ Extracted {len(df):,} rows from {config['filename']}")
        
        return df
    
    def extract_all(self) -> Dict[str, pd.DataFrame]:
        """Extract all configured source tables"""
        extracted = {}
        for table_name in self.SOURCE_FILES:
            extracted[table_name] = self.extract_table(table_name)
        return extracted
```

**Load: Write DataFrames to PostgreSQL database**

```python
# T0007: Implement demo script to read/write CSVs
# File: scripts/Load.py
"""
Load.py - Database Loading Module
Loads cleaned data to PostgreSQL database (etl_output schema).

Target: Docker Airflow PostgreSQL
- Host: postgres:5432 (Docker) / localhost:5432 (local)
- Database: airflow
- Schema: etl_output (separate from Airflow tables)
"""
import pandas as pd
from sqlalchemy import create_engine, text

class DatabaseLoader:
    """TEAM 1 - Database Loading Handler"""
    
    ETL_SCHEMA = 'etl_output'
    
    TABLE_CONFIG = {
        'customers': {'table_name': 'customers', 'primary_key': 'CustomerKey'},
        'sales': {'table_name': 'sales', 'primary_key': 'Order Number'},
        'products': {'table_name': 'products', 'primary_key': 'ProductKey'},
        'stores': {'table_name': 'stores', 'primary_key': 'StoreKey'},
        'exchange_rates': {'table_name': 'exchange_rates', 'primary_key': ['Date', 'Currency']},
    }
    
    def __init__(self, connection_string: str = None, batch_size: int = 5000):
        self.connection_string = connection_string or \
            "postgresql://airflow:airflow@localhost:5432/airflow"
        self.batch_size = batch_size
        self.engine = None
    
    def load_table(self, table_key: str, df: pd.DataFrame, mode: str = 'replace'):
        """
        Load a single table to database (etl_output schema)
        
        Args:
            table_key: Key from TABLE_CONFIG (customers, sales, etc.)
            df: DataFrame to load
            mode: 'replace' (TRUNCATE+INSERT), 'append', or 'upsert'
        """
        config = self.TABLE_CONFIG[table_key]
        table_name = config['table_name']
        
        engine = create_engine(self.connection_string)
        
        # Create schema if needed
        with engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.ETL_SCHEMA}"))
            if mode == 'replace':
                conn.execute(text(f"DROP TABLE IF EXISTS {self.ETL_SCHEMA}.{table_name}"))
            conn.commit()
        
        # Write to database
        df.to_sql(table_name, engine, schema=self.ETL_SCHEMA, 
                  if_exists='replace' if mode == 'replace' else 'append', index=False)
        
        print(f"✅ Loaded {len(df):,} rows to {self.ETL_SCHEMA}.{table_name}")
```

## T0008) Cleaning Utilities (trim, fillna, typecast)

**Reusable cleaning module:**

```python
# File: scripts/cleaning_utils.py
import pandas as pd
import numpy as np


class DataCleaner:
    """Reusable data cleaning utilities for pandas DataFrames."""

    # ========================================
    # Team 1 - T0008: Build reusable cleaning utilities (trim)
    # ========================================
    @staticmethod
    def trim_whitespace(df: pd.DataFrame) -> pd.DataFrame:
        """Trim leading/trailing whitespace from all string columns."""
        df = df.copy()
        str_cols = df.select_dtypes(include="object").columns
        for col in str_cols:
            if df[col].dtype == "object":
                df[col] = df[col].str.strip()
        return df

    # ========================================
    # Team 1 - T0008: Build reusable cleaning utilities (typecast)
    # ========================================
    @staticmethod
    def typecast_column(df: pd.DataFrame, column: str, dtype: str) -> pd.DataFrame:
        """Cast a column to a specified data type."""
        df = df.copy()
        if column in df.columns:
            if dtype == "int":
                df[column] = df[column].astype("Int64")  # Nullable int
            elif dtype == "float":
                df[column] = df[column].astype("Float64")
            elif dtype == "datetime":
                df[column] = pd.to_datetime(df[column], errors="coerce")
            else:
                df[column] = df[column].astype(dtype)
        return df

    # ========================================
    # Team 1 - T0008: Build reusable cleaning utilities (empty strings)
    # ========================================
    @staticmethod
    def remove_empty_strings(df: pd.DataFrame) -> pd.DataFrame:
        """Convert empty strings and whitespace-only strings to NaN."""
        df = df.copy()
        str_cols = df.select_dtypes(include="object").columns
        for col in str_cols:
            df[col] = df[col].replace(r"^\s*$", np.nan, regex=True)
        return df
```

**Usage in ETL DAGs:**

```python
# File: dags/etl_customers.py - transform_customers()

    # ========================================
    # Team 1 - T0008: Build reusable cleaning utilities (typecast)
    # Team 1 - T0011: Missing data handling (mean fill)
    # ========================================
    if 'Age' in df.columns:
        df['Age'] = pd.to_numeric(df['Age'], errors='coerce')
        mean_age = df['Age'].mean()
        df['Age'] = df['Age'].fillna(mean_age).astype(int)
```
            df[col] = df[col].replace(r"^\s*$", np.nan, regex=True)
        return df
```

**Usage in ETL DAGs:**

```python
# File: dags/etl_customers.py - transform_customers()
# Typecast and fill age with mean
if 'Age' in df.columns:
    df['Age'] = pd.to_numeric(df['Age'], errors='coerce')
    mean_age = df['Age'].mean()
    df['Age'] = df['Age'].fillna(mean_age).astype(int)
```

## T0009) Handle Incorrect Data Types

**Data type detection and validation:**

```python
# File: scripts/utils/validation_utils.py
import pandas as pd
import numpy as np
import re


class DataValidator:
    """Reusable data validation utilities"""
    
    # ========================================
    # Team 1 - T0009: Handle incorrect data types (detection)
    # ========================================
    @staticmethod
    def detect_data_types(df: pd.DataFrame) -> Dict[str, str]:
        """Detect and infer data types for columns."""
        type_map = {}
        
        for col in df.columns:
            non_null = df[col].dropna()
            if len(non_null) == 0:
                type_map[col] = 'unknown'
                continue
            
            # Check for numeric (>90% convertible)
            numeric_valid = pd.to_numeric(non_null, errors='coerce').notna().sum()
            if numeric_valid / len(non_null) > 0.9:
                type_map[col] = 'int' if str(non_null.iloc[0]).isdigit() else 'float'
                continue
            
            # Check for date columns by name
            if col.lower() in ['date', 'birthday', 'order_date', 'delivery_date']:
                type_map[col] = 'date'
                continue
            
            type_map[col] = 'string'
        
        return type_map
    
    # ========================================
    # Team 1 - T0009: Handle incorrect data types (email validation)
    # ========================================
    @staticmethod
    def validate_email(email: str) -> bool:
        """Validate email format."""
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, str(email)))
```

**Usage in ETL DAGs:**

```python
# File: dags/etl_customers.py - transform_customers()

    # ========================================
    # Team 1 - T0009: Handle incorrect data types (birthday)
    # ========================================
    if 'Birthday' in df.columns:
        df['Birthday'] = pd.to_datetime(df['Birthday'], errors='coerce')

    # ========================================
    # Team 1 - T0009: Handle incorrect data types (email validation)
    # ========================================
    if 'Email' in df.columns:
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        
        def validate_email(email):
            if pd.isna(email):
                return 'no-email@unknown.com'
            if re.match(email_pattern, str(email)):
                return email
            return 'invalid-email@unknown.com'
        
        df['Email'] = df['Email'].apply(validate_email)
```

## T0010) Duplicate Data Detection & Removal

**Reusable module:**

```python
# File: scripts/cleaning_utils.py
from typing import Literal


class DataCleaner:
    # ========================================
    # Team 1 - T0010: Duplicate data detection & removal
    # ========================================
    @staticmethod
    def remove_duplicates(
        df: pd.DataFrame,
        subset: list[str] | None = None,
        keep: Literal["first", "last", False] = "first",
    ) -> pd.DataFrame:
        """Remove duplicate rows.

        Args:
            df: Input DataFrame
            subset: Column(s) to consider for identifying duplicates
            keep: 'first', 'last', or False (remove all duplicates)

        Returns:
            DataFrame with duplicates removed
        """
        return df.drop_duplicates(subset=subset, keep=keep)
```

**Usage in ETL DAGs:**

```python
# File: dags/etl_customers.py - transform_customers()

    # ========================================
    # Team 1 - T0010: Duplicate data detection & removal
    # ========================================
    df = df.drop_duplicates(subset=['CustomerKey'], keep='first')
    duplicates_removed = initial_rows - len(df)
    print(f"   Duplicates removed: {duplicates_removed}")

# File: dags/etl_sales.py - transform_sales()
    df = df.drop_duplicates(subset=['Order Number'], keep='first')

# File: dags/etl_products.py - transform_products()
    df = df.drop_duplicates(subset=['ProductKey'], keep='first')
```

## T0011) Missing Data Handling (mean, drop)

**Reusable module with multiple strategies:**

```python
# File: scripts/cleaning_utils.py
from typing import Literal


class DataCleaner:
    # ========================================
    # Team 1 - T0011: Missing data handling (mean fill)
    # ========================================
    @staticmethod
    def fill_missing_mean(df: pd.DataFrame, columns: list[str] | None = None) -> pd.DataFrame:
        """Fill missing numeric values with column mean."""
        df = df.copy()
        if columns is None:
            columns = df.select_dtypes(include=[np.number]).columns.tolist()
        for col in columns:
            if col in df.columns:
                df[col] = df[col].fillna(df[col].mean())
        return df

    # ========================================
    # Team 1 - T0011: Missing data handling (median fill)
    # ========================================
    @staticmethod
    def fill_missing_median(df: pd.DataFrame, columns: list[str] | None = None) -> pd.DataFrame:
        """Fill missing numeric values with column median."""
        df = df.copy()
        if columns is None:
            columns = df.select_dtypes(include=[np.number]).columns.tolist()
        for col in columns:
            if col in df.columns:
                df[col] = df[col].fillna(df[col].median())
        return df

    @staticmethod
    def handle_missing_data(
        df: pd.DataFrame,
        strategy: Literal["drop", "mean", "median", "forward_fill", "backward_fill"] = "drop",
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """Generic missing data handler with configurable strategy."""
        df = df.copy()
        if columns is None:
            columns = df.columns.tolist()
        if strategy == "drop":
            df = df.dropna(subset=columns)
        elif strategy == "mean":
            for col in columns:
                if col in df.columns and df[col].dtype in [np.float64, np.int64]:
                    df[col] = df[col].fillna(df[col].mean())
        elif strategy == "median":
            for col in columns:
                if col in df.columns and df[col].dtype in [np.float64, np.int64]:
                    df[col] = df[col].fillna(df[col].median())
        return df
```

**Usage in ETL DAGs:**

```python
# File: dags/etl_customers.py - transform_customers()

    # ========================================
    # Team 1 - T0008: Build reusable cleaning utilities (typecast)
    # Team 1 - T0011: Missing data handling (mean fill)
    # ========================================
    if 'Age' in df.columns:
        df['Age'] = pd.to_numeric(df['Age'], errors='coerce')
        mean_age = df['Age'].mean()
        df['Age'] = df['Age'].fillna(mean_age).astype(int)

# File: dags/etl_sales.py - transform_sales()
    # Remove invalid records (negative quantities)
    if 'Quantity' in df.columns:
        invalid = len(df[df['Quantity'] <= 0])
        df = df[df['Quantity'] > 0]
        print(f"   Removed {invalid} records with invalid quantity")
```

## T0012) Config-Driven Cleaning Rules

**Config files per source table:**

```yaml
# File: config/customers_config.yaml
###########  T0012: Customers Cleaning Rules Config #############

source:
  type: 'postgres'
  csv_file: 'data/raw/dataset/Customers.csv'

destination:
  type: 'postgres'
  table: 'output_customers_cleaned'
  mode: 'upsert'
  merge_keys: ['customer_key']

cleaning_rules:
  duplicates:
    enabled: true
    subset: ['customer_key']
    keep: 'first'
    log_to_rejects: true
  
  date_columns:
    birthday:
      input_formats: ['%m/%d/%Y', '%d-%m-%Y', '%d/%m/%Y', '%Y-%m-%d']
      output_format: '%d-%m-%Y'
      handle_invalid: 'reject'
  
  type_conversions:
    age:
      target_type: 'int'
      handle_errors: 'reject'
  
  missing_values:
    age:
      strategy: 'mean'
      scope: 'global'
    email:
      strategy: 'fill'
      fill_value: 'noemail@company.com'
  
  validations:
    email:
      pattern: '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
      handle_invalid: 'replace'
      replacement_value: 'invalid@company.com'

quality_checks:
  - check: 'no_duplicates'
    column: 'customer_key'
  - check: 'date_format'
    column: 'birthday'
    format: '%d-%m-%Y'
  - check: 'type_match'
    column: 'age'
    expected_type: 'int'
```

**Additional config files available:**
- `config/sales_config.yaml` - Sales table cleaning rules
- `config/products_config.yaml` - Products table cleaning rules
- `config/stores_config.yaml` - Stores table cleaning rules
- `config/exchange_rates_config.yaml` - Exchange rates cleaning rules

**Config loader module:**

```python
# File: scripts/config_loader.py

class ConfigLoader:
    # ========================================
    # Team 1 - T0012: Config-driven cleaning rules (YAML loader)
    # ========================================
    @staticmethod
    def load_yaml(path: PathLike) -> dict[str, Any]:
        """Load a YAML file and return a dictionary."""
        p = ConfigLoader._ensure_exists(path)
        with p.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return data or {}
```
class ConfigLoader:
    @staticmethod
    def load_yaml(path: PathLike) -> dict[str, Any]:
        """Load a YAML file and return a dictionary."""
        p = ConfigLoader._ensure_exists(path)
        if yaml is None:
            raise ImportError("PyYAML is required to load YAML files.")
        with p.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return data or {}

    @staticmethod
    def load_json(path: PathLike) -> dict[str, Any]:
        """Load a JSON file and return a dictionary."""
        p = ConfigLoader._ensure_exists(path)
        with p.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {"_": data}
```

## T0013) Aggregations (groupBy, sum, min, max)

**Aggregation utilities module:**

```python
# File: scripts/utils/aggregation_utils.py
# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 3: Aggregation Utilities
# Tasks: T0013
# ═══════════════════════════════════════════════════════════════════════

import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Union
import logging

logger = logging.getLogger(__name__)


class AggregationEngine:
    """Performs data aggregations with flexible grouping and output options"""
    
    # ========================================
    # Team 1 - T0013: Aggregations (groupBy, sum, min, max)
    # ========================================
    @staticmethod
    def aggregate(df: pd.DataFrame,
                  group_by: Union[str, List[str]],
                  agg_config: Dict[str, Union[str, List[str]]],
                  batch_size: Optional[int] = None) -> pd.DataFrame:
        """
        Perform aggregation on DataFrame
        
        Args:
            df: Input DataFrame
            group_by: Column(s) to group by (string or list of strings)
            agg_config: Dict mapping column names to aggregation function(s)
                       e.g., {'Quantity': ['sum', 'count'], 'Price': 'avg'}
            batch_size: Optional batch size for memory optimization
        
        Example:
            >>> agg_config = {
            ...     'Quantity': ['sum', 'count', 'mean'],
            ...     'Price': ['min', 'max', 'median']
            ... }
            >>> result = AggregationEngine.aggregate(df, 'CustomerKey', agg_config)
        """
        if batch_size:
            logger.info(f"Processing {len(df)} rows in batches of {batch_size}")
            return AggregationEngine._aggregate_batched(df, group_by, agg_config, batch_size)
        
        grouped = df.groupby(group_by, as_index=False)
        result = grouped.agg(agg_config)
        
        # Flatten column names if multi-level
        if isinstance(result.columns, pd.MultiIndex):
            result.columns = ['_'.join(col).strip('_') for col in result.columns.values]
        
        logger.info(f"✅ Aggregation complete: {len(df)} → {len(result)} rows")
        return result
```

**Usage in ETL reports:**

```python
# File: dags/etl_reports.py - create_sales_summary()
# State-wise aggregations for reports
store_sales = sales.groupby('StoreKey').agg({
    'Order Number': 'count',
    'Quantity': 'sum'
}).reset_index()
store_sales.columns = ['StoreKey', 'Total_Orders', 'Total_Quantity']
```

## T0014) Normalization & Scaling

**Normalization utilities module:**

```python
# File: scripts/utils/normalization_utils.py
# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 3: Normalization Utilities
# Tasks: T0014
# ═══════════════════════════════════════════════════════════════════════

import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class NormalizationEngine:
    """
    Data normalization and scaling operations
    Focus on Z-score standardization as per requirements
    """
    
    # ========================================
    # Team 1 - T0014: Normalization & Scaling (Z-score)
    # ========================================
    @staticmethod
    def z_score_normalize(df: pd.DataFrame,
                         columns: Optional[List[str]] = None,
                         exclude_columns: Optional[List[str]] = None,
                         batch_size: Optional[int] = None,
                         add_as_new_column: bool = True,
                         suffix: str = '_zscore') -> Tuple[pd.DataFrame, Dict[str, Dict[str, float]]]:
        """
        Apply Z-score standardization (mean=0, std=1)
        
        IMPORTANT: By default, z-score is added as a NEW column (e.g., Quantity -> Quantity_zscore)
                   Original values are PRESERVED for analysis
        
        Formula: z = (x - mean) / std
        
        Args:
            df: Input DataFrame
            columns: Specific columns to normalize (default: all numeric)
            exclude_columns: Columns to exclude from normalization
            add_as_new_column: If True (default), adds z-score as new column, preserving original
            suffix: Suffix for new z-score columns (default: '_zscore')
        
        Returns:
            (normalized_df, normalization_stats)
        """
        df_normalized = df.copy()
        stats = {}
        
        # Auto-detect numeric columns if not specified
        if columns is None:
            columns = df.select_dtypes(include=[np.number]).columns.tolist()
            key_columns = ['CustomerKey', 'ProductKey', 'StoreKey', 'Order Number']
            columns = [col for col in columns if col not in key_columns]
        
        for col in columns:
            mean_val = df_normalized[col].mean()
            std_val = df_normalized[col].std()
            
            if std_val == 0:
                logger.warning(f"⚠️ Column {col} has std=0, skipping")
                continue
            
            if add_as_new_column:
                df_normalized[f'{col}{suffix}'] = (df_normalized[col] - mean_val) / std_val
            else:
                df_normalized[col] = (df_normalized[col] - mean_val) / std_val
            
            stats[col] = {'mean': mean_val, 'std': std_val}
        
        return df_normalized, stats
```

## T0015) Feature Engineering Logic

**Feature engineering utilities module:**

```python
# File: scripts/utils/feature_engineering_utils.py
# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 3: Feature Engineering Utilities
# Tasks: T0015
# ═══════════════════════════════════════════════════════════════════════

import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, List
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class FeatureEngine:
    """Feature engineering for customers, sales, and products"""
    
    # ========================================
    # Team 1 - T0015: Feature Engineering (customer features)
    # ========================================
    @staticmethod
    def create_customer_features(customers_df: pd.DataFrame,
                                 sales_df: pd.DataFrame,
                                 customer_key: str = 'CustomerKey',
                                 order_date_col: str = 'Order Date',
                                 quantity_col: str = 'Quantity',
                                 reference_date: Optional[datetime] = None) -> pd.DataFrame:
        """
        Create comprehensive customer features
        
        Features created:
        - total_orders: Total number of orders
        - total_quantity: Total quantity purchased
        - avg_order_quantity: Average quantity per order
        - lifetime_value: Total purchase value (if available)
        - purchase_frequency: Orders per month
        - recency_days: Days since last purchase
        - tenure_days: Days since first purchase
        - is_active: Active in last 90 days
        """
        customers_enhanced = customers_df.copy()
        
        if reference_date is None:
            reference_date = datetime.now()
        
        # Aggregate sales by customer
        customer_stats = sales_df.groupby(customer_key).agg({
            order_date_col: ['count', 'min', 'max'],
            quantity_col: ['sum', 'mean']
        }).reset_index()
        
        # Flatten column names
        customer_stats.columns = [
            customer_key, 'total_orders', 'first_purchase_date',
            'last_purchase_date', 'total_quantity', 'avg_order_quantity'
        ]
        
        # Calculate derived features
        customer_stats['tenure_days'] = (
            reference_date - customer_stats['first_purchase_date']
        ).dt.days
        
        customer_stats['recency_days'] = (
            reference_date - customer_stats['last_purchase_date']
        ).dt.days
        
        customer_stats['is_active'] = customer_stats['recency_days'] <= 90
        
        # Merge with customers
        customers_enhanced = customers_enhanced.merge(
            customer_stats, on=customer_key, how='left'
        )
        
        return customers_enhanced
```

## T0016) Date/Time Transformations

**DateTime utilities module:**

```python
# File: scripts/utils/datetime_utils.py
# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 3: DateTime Utilities
# Tasks: T0016
# ═══════════════════════════════════════════════════════════════════════

import pandas as pd
import numpy as np
from typing import Optional, List, Dict
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class DateTimeEngine:
    """Date/time feature extraction and interval calculations"""
    
    # ========================================
    # Team 1 - T0016: Date/Time Transformations (date parts)
    # ========================================
    @staticmethod
    def extract_date_parts(df: pd.DataFrame,
                          date_col: str,
                          prefix: Optional[str] = None) -> pd.DataFrame:
        """
        Extract basic date parts: year, month, day, day_of_week, quarter
        """
        df_new = df.copy()
        
        if prefix is None:
            prefix = date_col.lower().replace(' ', '_')
        
        # Ensure datetime
        if not pd.api.types.is_datetime64_any_dtype(df_new[date_col]):
            df_new[date_col] = pd.to_datetime(df_new[date_col])
        
        df_new[f'{prefix}_year'] = df_new[date_col].dt.year
        df_new[f'{prefix}_month'] = df_new[date_col].dt.month
        df_new[f'{prefix}_day'] = df_new[date_col].dt.day
        df_new[f'{prefix}_day_of_week'] = df_new[date_col].dt.dayofweek
        df_new[f'{prefix}_quarter'] = df_new[date_col].dt.quarter
        
        logger.info(f"✅ Extracted date parts for {date_col}")
        return df_new
    
    # ========================================
    # Team 1 - T0016: Date/Time Transformations (intervals)
    # ========================================
    @staticmethod
    def days_between(df: pd.DataFrame,
                    start_col: str,
                    end_col: str,
                    output_col: str = 'days_between') -> pd.DataFrame:
        """
        Calculate days between two date columns
        """
        df_new = df.copy()
        
        # Ensure datetime
        for col in [start_col, end_col]:
            if not pd.api.types.is_datetime64_any_dtype(df_new[col]):
                df_new[col] = pd.to_datetime(df_new[col])
        
        df_new[output_col] = (df_new[end_col] - df_new[start_col]).dt.days
        
        logger.info(f"✅ Calculated {output_col} between {start_col} and {end_col}")
        return df_new
```

## T0017) Transformation Orchestrator

**Unified transformation pipeline orchestration:**

```python
# File: scripts/utils/transformation_orchestrator.py
# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 3: Transformation Orchestrator
# Tasks: T0017
# ═══════════════════════════════════════════════════════════════════════

import pandas as pd
import yaml
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime

# Import utility engines
from .aggregation_utils import AggregationEngine
from .normalization_utils import NormalizationEngine
from .feature_engineering_utils import FeatureEngine
from .datetime_utils import DateTimeEngine

logger = logging.getLogger(__name__)


class TransformationOrchestrator:
    """
    Main orchestration engine for Phase 3 transformations
    
    Flow:
    1. Load config from YAML
    2. Load source data
    3. Apply aggregations (if enabled)
    4. Apply normalization (if enabled)
    5. Apply feature engineering (if enabled)
    6. Apply datetime transformations (if enabled)
    7. Save outputs (CSV and/or DB)
    8. Generate report
    """
    
    def __init__(self, config_path: str):
        """Initialize orchestrator with configuration"""
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self.stats = {}
        logger.info(f"✅ Orchestrator initialized with config: {config_path}")
    
    def _load_config(self) -> Dict[str, Any]:
        """Load and parse YAML configuration"""
        with open(self.config_path, 'r') as f:
            config = yaml.safe_load(f)
        return config
    
    # ========================================
    # Team 1 - T0017: Transformation Orchestrator (pipeline)
    # ========================================
    def run(self,
            source: str,
            data_path: str,
            output_dir: str = 'data/transformed',
            save_csv: bool = True,
            save_db: bool = True,
            db_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Execute full transformation pipeline for a source
        
        Args:
            source: Source name (e.g., 'amazon', 'customers')
            data_path: Path to input CSV or data source
            output_dir: Directory for CSV outputs
            save_csv: Whether to save to CSV
            save_db: Whether to save to SQLite
            db_path: Path to SQLite database
        
        Returns:
            Transformation results and metrics
        """
        start_time = datetime.now()
        results = {'source': source, 'stages': {}}
        
        # 1. Load data
        df = pd.read_csv(data_path)
        results['input_rows'] = len(df)
        
        # 2. Get source config
        source_config = self.config.get('sources', {}).get(source, {})
        
        # 3. Apply transformations in sequence
        df = self._apply_aggregations(df, source_config, results)
        df = self._apply_normalization(df, source_config, results)
        df = self._apply_features(df, source_config, results)
        df = self._apply_datetime(df, source_config, results)
        
        results['output_rows'] = len(df)
        results['elapsed_seconds'] = (datetime.now() - start_time).total_seconds()
        results['status'] = 'SUCCESS'
        
        return results
```

**Config file for orchestrator:**

```yaml
# File: config/transformation_config.yaml
sources:
  amazon:
    aggregations:
      enabled: true
      group_by: ['CustomerKey']
      metrics:
        Quantity: ['sum', 'count', 'mean']
    
    normalization:
      enabled: true
      columns: ['Quantity', 'TotalAmount']
      method: 'zscore'
    
    feature_engineering:
      enabled: true
      customer_features: true
    
    datetime:
      enabled: true
      extract_parts: ['Order Date']
```

## T0018) Bulk Load Operations

**Bulk loader utilities module:**

```python
# File: scripts/utils/bulk_loader.py
# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 4: Bulk Loader Utility
# Tasks: T0018
# ═══════════════════════════════════════════════════════════════════════

import pandas as pd
from sqlalchemy import create_engine, text
from typing import Dict, Any, Optional, Callable
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class BulkLoader:
    """TEAM 1 - T0018: High-performance bulk data loader"""
    
    DEFAULT_CONNECTION = "postgresql://airflow:airflow@localhost:5432/airflow"
    
    def __init__(self, 
                 connection_string: Optional[str] = None,
                 batch_size: int = 10000,
                 use_transactions: bool = True):
        self.connection_string = connection_string or self.DEFAULT_CONNECTION
        self.batch_size = batch_size
        self.use_transactions = use_transactions
        self.stats = {'total_rows': 0, 'loaded_rows': 0, 'failed_rows': 0}
        logger.info(f"▶ TEAM 1 - T0018: BulkLoader initialized (batch_size={batch_size})")
    
    # ========================================
    # Team 1 - T0018: Bulk Load Operations
    # ========================================
    def bulk_load(self, df: pd.DataFrame, table_name: str, 
                  schema: str = 'etl_output',
                  if_exists: str = 'append',
                  progress_callback: Optional[Callable] = None) -> Dict[str, Any]:
        """Load DataFrame to database in batches"""
        self.stats['total_rows'] = len(df)
        self.stats['start_time'] = datetime.now()
        
        engine = self.connect()
        total_batches = (len(df) + self.batch_size - 1) // self.batch_size
        
        # Process in chunks for memory efficiency
        for batch_num, start_idx in enumerate(range(0, len(df), self.batch_size), 1):
            chunk = df.iloc[start_idx:start_idx + self.batch_size]
            batch_if_exists = if_exists if batch_num == 1 else 'append'
            
            chunk.to_sql(
                table_name, engine, schema=schema,
                if_exists=batch_if_exists, index=False,
                method='multi'  # Multi-row insert for performance
            )
            
            self.stats['loaded_rows'] += len(chunk)
            logger.info(f"   Batch {batch_num}/{total_batches}: {len(chunk):,} rows")
        
        self.stats['end_time'] = datetime.now()
        return self.stats
```

**Usage in ETL DAGs:**

```python
# File: dags/etl_customers.py (load_customers function)
df.to_sql(
    'customers',
    engine,
    schema='etl_output',
    if_exists='replace',
    index=False
)
```

## T0019) Incremental vs Full Loads

**Load strategy utilities module:**

```python
# File: scripts/utils/load_strategy.py
# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 4: Load Strategy Utility
# Tasks: T0019
# ═══════════════════════════════════════════════════════════════════════

from enum import Enum
import logging

logger = logging.getLogger(__name__)


class LoadType(Enum):
    """Load strategy types"""
    FULL = "full"           # Truncate and reload all data
    INCREMENTAL = "incremental"  # Load only new/changed records
    APPEND = "append"       # Append without checking duplicates


class LoadStrategy:
    """TEAM 1 - T0019: Data loading strategy manager"""
    
    # Table configurations with primary keys
    TABLE_CONFIGS = {
        'customers': {'primary_key': 'CustomerKey', 'supports_incremental': True},
        'sales': {'primary_key': 'Order Number', 'date_column': 'Order Date', 'supports_incremental': True},
        'products': {'primary_key': 'ProductKey', 'supports_incremental': True},
        'stores': {'primary_key': 'StoreKey', 'supports_incremental': True},
        'exchange_rates': {'primary_key': 'Date', 'supports_incremental': True}
    }
    
    # ========================================
    # Team 1 - T0019: Incremental vs Full Loads
    # ========================================
    def determine_load_type(self,
                            table_name: str,
                            force_full: bool = False,
                            force_incremental: bool = False) -> LoadType:
        """Determine the appropriate load type for a table"""
        if force_full:
            logger.info(f"   {table_name}: Forced FULL load")
            return LoadType.FULL
        
        if force_incremental:
            logger.info(f"   {table_name}: Forced INCREMENTAL load")
            return LoadType.INCREMENTAL
        
        # Check table config
        config = self.TABLE_CONFIGS.get(table_name.lower(), {})
        if not config.get('supports_incremental', False):
            return LoadType.FULL
        
        return LoadType.INCREMENTAL
    
    def find_new_records(self, df, table_name, schema='etl_output'):
        """Find records not in target table (based on primary key)"""
        config = self.TABLE_CONFIGS.get(table_name.lower(), {})
        pk = config.get('primary_key')
        # Compare with existing DB records
        logger.info(f"▶ TEAM 1 - T0019: Finding new records for {table_name}")
        # ... implementation
```

## T0020) Handling Constraint Violations

**Constraint handler utilities module:**

```python
# File: scripts/utils/constraint_handler.py
# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 4: Constraint Handler
# Tasks: T0020
# ═══════════════════════════════════════════════════════════════════════

from enum import Enum
from typing import List, Tuple, Dict, Any
from sqlalchemy.exc import IntegrityError
import logging

logger = logging.getLogger(__name__)


class ConstraintType(Enum):
    PRIMARY_KEY = "primary_key"
    FOREIGN_KEY = "foreign_key"
    UNIQUE = "unique"
    NOT_NULL = "not_null"


class ViolationAction(Enum):
    SKIP = "skip"       # Skip the record, continue with others (Option A)
    STOP = "stop"       # Stop entire load operation


class ConstraintHandler:
    """
    TEAM 1 - T0020: Database constraint violation handler
    Behavior: Option A - Log error, skip record, continue loading others
    """
    
    # ========================================
    # Team 1 - T0020: Handling Constraint Violations
    # ========================================
    def validate_before_load(self,
                             df: pd.DataFrame,
                             table_name: str,
                             schema: str = 'public') -> Tuple[pd.DataFrame, pd.DataFrame, Dict]:
        """Pre-load validation to catch issues before database insert"""
        logger.info(f"▶ TEAM 1 - T0020: Pre-load validation for {table_name}")
        # Validate primary keys, null constraints, data types
        valid_df = df[~df.duplicated(subset=[pk], keep='first')]
        rejected_df = df[df.duplicated(subset=[pk], keep='first')]
        return valid_df, rejected_df, stats
    
    def handle_constraint_error(self, chunk, error, table_name):
        """Handle constraint violation - Log, Skip, Continue"""
        logger.error(f"Constraint violation: {error}")
        # Insert individually to isolate failing rows
        for idx, row in chunk.iterrows():
            try:
                pd.DataFrame([row]).to_sql(table_name, conn, if_exists="append", index=False)
            except IntegrityError as row_err:
                self.rejected_rows.append((row.to_dict(), str(row_err)))
```

## T0021) Upsert Logic

**Upsert handler utilities module:**

```python
# File: scripts/utils/upsert_handler.py
# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 4: Upsert Handler
# Tasks: T0021
# ═══════════════════════════════════════════════════════════════════════

from sqlalchemy.dialects.postgresql import insert as pg_insert
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class UpsertMode(str):
    UPDATE = "update"       # Update all columns on conflict
    IGNORE = "ignore"       # Skip conflicting records
    REPLACE = "replace"     # Delete and re-insert


class UpsertHandler:
    """
    TEAM 1 - T0021: Upsert (Insert/Update) logic handler
    Uses PostgreSQL's ON CONFLICT clause for atomic upserts.
    """
    
    # Primary key configurations per table
    TABLE_PRIMARY_KEYS = {
        'customers': ['CustomerKey'],
        'sales': ['Order Number'],
        'products': ['ProductKey'],
        'stores': ['StoreKey'],
        'exchange_rates': ['Date', 'Currency']  # Composite key
    }
    
    # ========================================
    # Team 1 - T0021: Upsert Logic (Insert/Update)
    # ========================================
    def upsert(self, df: pd.DataFrame, table_name: str, 
               primary_keys: List[str],
               schema: str = 'etl_output') -> Dict:
        """Perform upsert using PostgreSQL ON CONFLICT DO UPDATE"""
        logger.info(f"▶ TEAM 1 - T0021: Starting upsert to {schema}.{table_name}")
        
        # Build upsert statement
        records = df.to_dict('records')
        stmt = pg_insert(table).values(records)
        
        # ON CONFLICT DO UPDATE for all non-key columns
        update_cols = {c.name: c for c in stmt.excluded if c.name not in primary_keys}
        stmt = stmt.on_conflict_do_update(
            index_elements=primary_keys, 
            set_=update_cols
        )
        
        engine.execute(stmt)
        return self.stats
```

## T0022) Error Table Creation (Rejects)

**Rejected records handler utilities module:**

```python
# File: scripts/utils/rejected_records_handler.py
# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 4: Rejected Records Handler
# Tasks: T0022
# ═══════════════════════════════════════════════════════════════════════

import json
import hashlib
from datetime import datetime
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class RejectedRecordsHandler:
    """TEAM 1 - T0022: Rejected records error table handler"""
    
    TABLE_NAME = "rejected_records"
    SCHEMA = "public"
    
    # Table DDL
    CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
        id SERIAL PRIMARY KEY,
        record_id VARCHAR(255),
        source_table VARCHAR(100) NOT NULL,
        error_type VARCHAR(100) NOT NULL,
        error_message TEXT,
        raw_data TEXT,
        rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        dag_run_id VARCHAR(255),
        task_id VARCHAR(255),
        retry_count INTEGER DEFAULT 0,
        status VARCHAR(50) DEFAULT 'pending',
        record_hash VARCHAR(64)
    );
    
    CREATE INDEX IF NOT EXISTS idx_rejected_source_table 
        ON {schema}.{table_name}(source_table);
    CREATE INDEX IF NOT EXISTS idx_rejected_status 
        ON {schema}.{table_name}(status);
    """
    
    # ========================================
    # Team 1 - T0022: Error Table Creation (Rejects)
    # ========================================
    def ensure_table_exists(self) -> bool:
        """Create rejected_records table if it doesn't exist"""
        engine = self.connect()
        with engine.begin() as conn:
            conn.execute(text(self.CREATE_TABLE_SQL.format(
                schema=self.schema, table_name=self.table_name
            )))
        logger.info(f"✅ Rejected records table ensured")
        return True
    
    def add_rejected_record(self, source_table: str, error_type: str,
                            error_message: str, raw_data: Dict,
                            dag_run_id: str = None, task_id: str = None):
        """Add a rejected record to the error table"""
        record_hash = hashlib.md5(json.dumps(raw_data, sort_keys=True).encode()).hexdigest()
        # Insert into rejected_records table
        logger.info(f"▶ TEAM 1 - T0022: Logging rejected record from {source_table}")
    
    def export_to_csv(self, output_path: str, status: str = None) -> str:
        """Export rejected records to CSV for analysis"""
        pass
```

---

## T0023) Build Master DAG to Trigger All Pipelines

```python
# T0023: Master orchestrator DAG with TaskGroups
# File: dags/etl_master_orchestrator.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup

# Child DAGs to orchestrate
INDEPENDENT_DAGS = ['etl_customers', 'etl_products', 'etl_stores', 'etl_exchange_rates']
DEPENDENT_DAGS = ['etl_sales']  # Depends on etl_products
FINAL_DAGS = ['etl_reports']    # Depends on ALL table DAGs

with DAG(
    dag_id='etl_master_orchestrator',
    default_args={**DEFAULT_ARGS, 'on_failure_callback': send_failure_email},
    description='Master Orchestrator - Triggers and monitors all ETL pipelines',
    start_date=START_DATE,
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=['team1', 'orchestrator', 'master'],
) as dag:
    
    # Stage 1: Dimension Tables (Parallel) with TaskGroup
    with TaskGroup(
        group_id='stage1_dimension_tables',
        tooltip='Load dimension tables: Customers, Products, Stores, Exchange Rates'
    ) as stage1_group:
        
        # Customers sub-group
        with TaskGroup(group_id='customers') as customers_group:
            trigger_customers = TriggerDagRunOperator(
                task_id='trigger',
                trigger_dag_id='etl_customers',
                wait_for_completion=False,
                reset_dag_run=True,
            )
            wait_customers = ExternalTaskSensor(
                task_id='wait_complete',
                external_dag_id='etl_customers',
                external_task_id='end',
                allowed_states=['success'],
                execution_date_fn=lambda dt: get_latest_execution_date(dt, 'etl_customers'),
                timeout=3600,
                mode='reschedule',
            )
            trigger_customers >> wait_customers
        
        # Similar pattern for: products_group, stores_group, exchange_rates_group
        # All run in parallel within Stage 1
```

## T0024) Event-Driven DAG Triggering

**ExternalTaskSensor for event-driven triggering:**

```python
# File: dags/etl_sales.py
# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - PHASE 5: ETL SALES DAG
# Tasks: T0023-T0027, especially T0024-T0025 (Sprint 5)
# ═══════════════════════════════════════════════════════════════════════

from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import DagRun
from airflow.utils.state import DagRunState


def get_latest_execution_date(dt, external_dag_id):
    """Get the latest successful execution date for an external DAG."""
    dag_runs = DagRun.find(
        dag_id=external_dag_id,
        state=DagRunState.SUCCESS,
    )
    if dag_runs:
        latest_run = max(dag_runs, key=lambda x: x.execution_date)
        return latest_run.execution_date
    return dt


# ========================================
# Team 1 - T0024: Event-Driven DAG Triggering
# ========================================

with DAG(dag_id='etl_sales', ...):
    # Wait for Products DAG to complete (Sales references Products)
    wait_for_products = ExternalTaskSensor(
        task_id='wait_for_products',
        external_dag_id='etl_products',
        external_task_id='end',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        execution_date_fn=lambda dt: get_latest_execution_date(dt, 'etl_products'),
        timeout=7200,  # 2 hours
        poke_interval=30,  # Check every 30 seconds
        mode='reschedule',  # Free worker while waiting
    )
    
    # Pipeline only starts after Products DAG succeeds
    start >> wait_for_products >> extract >> transform >> load >> end
```

## T0025) Multi-DAG Dependency Management

**5 ExternalTaskSensors for cross-DAG dependencies:**

```python
# File: dags/etl_reports.py
# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - PHASE 5: ETL REPORTS DAG (MASTER)
# Tasks: T0023-T0027, T0031 (Sprint 5 & 6)
# ═══════════════════════════════════════════════════════════════════════
"""
etl_reports.py - Report Generation Pipeline (MASTER DAG)
Dependencies: WAITS FOR ALL 5 table DAGs:
  - etl_customers
  - etl_products
  - etl_stores
  - etl_exchange_rates
  - etl_sales
"""
from airflow.sensors.external_task import ExternalTaskSensor

with DAG(dag_id='etl_reports', ...):
    
    start = EmptyOperator(task_id='start')
    
    # ========================================
    # Team 1 - T0025: Multi-DAG Dependency Management
    # (5 ExternalTaskSensors - wait for ALL table DAGs)
    # ========================================
    
    sensor_timeout = 7200  # 2 hours
    sensor_poke = 30  # Check every 30 seconds
    
    # Wait for all 5 table DAGs to complete
    wait_for_customers = ExternalTaskSensor(
        task_id='wait_for_customers',
        external_dag_id='etl_customers',
        external_task_id='end',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        execution_date_fn=lambda dt: get_latest_execution_date(dt, 'etl_customers'),
        timeout=sensor_timeout,
        poke_interval=sensor_poke,
        mode='reschedule',
    )
    
    wait_for_products = ExternalTaskSensor(...)
    wait_for_stores = ExternalTaskSensor(...)
    wait_for_exchange_rates = ExternalTaskSensor(...)
    wait_for_sales = ExternalTaskSensor(...)
    
    # Report generation only runs after ALL sensors pass
    start >> [wait_for_customers, wait_for_products, wait_for_stores, 
              wait_for_exchange_rates, wait_for_sales] >> generate_reports >> end
```

## T0026) Backfill & Catchup Features

**DAG configuration for backfill and catchup:**

```python
# File: dags/dag_base.py
# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - PHASE 5: ETL DAG BASE MODULE
# Tasks: T0023, T0026, T0027 (Sprint 5)
# ═══════════════════════════════════════════════════════════════════════

from datetime import datetime, timedelta

# Schedule: Midnight IST = 18:30 UTC
SCHEDULE_MIDNIGHT_IST = '30 18 * * *'

# Start date for catchup
START_DATE = datetime(2025, 12, 25)

# ========================================
# Team 1 - T0026: Backfill & Catchup Features
# ========================================
# Common DAG configuration
DAG_CONFIG = {
    'start_date': START_DATE,
    'schedule_interval': SCHEDULE_MIDNIGHT_IST,
    'catchup': True,          # Enable automatic backfill
    'max_active_runs': 3,     # Limit concurrent backfill runs
    'tags': ['team1', 'etl', 'amazon'],
}
```

**Manual backfill via CLI:**
```bash
# Backfill specific date range
airflow dags backfill etl_customers -s 2025-12-01 -e 2025-12-25

# Backfill with reset (clear existing runs first)
airflow dags backfill etl_customers -s 2025-12-01 -e 2025-12-25 --reset-dagruns
```

## T0027) DAG Failure Handling Strategy

**Failure handling with retries and email callbacks:**

```python
# File: dags/dag_base.py

from airflow.utils.email import send_email
import os

ALERT_EMAIL = os.environ.get('SMTP_USER', 'sidiot6969@gmail.com')

# Common default args for all DAGs
DEFAULT_ARGS = {
    'owner': 'team1',
    'depends_on_past': False,
    'email': [ALERT_EMAIL],
    'email_on_failure': False,  # Disabled for testing
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(hours=2),
}


# ========================================
# Team 1 - T0027: DAG Failure Handling Strategy
# ========================================

def send_success_email(context):
    """Send email on successful DAG completion"""
    dag_id = context['dag'].dag_id
    execution_date = context['execution_date']
    
    subject = f"✅ ETL Success: {dag_id}"
    body = f"""
    <h2>ETL Pipeline Completed Successfully</h2>
    <table border="1" cellpadding="5">
        <tr><td><b>DAG</b></td><td>{dag_id}</td></tr>
        <tr><td><b>Execution Date</b></td><td>{execution_date}</td></tr>
        <tr><td><b>Status</b></td><td style="color:green;">SUCCESS</td></tr>
    </table>
    """
    send_email(to=ALERT_EMAIL, subject=subject, html_content=body)


def send_failure_email(context):
    """Send email on task failure"""
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    execution_date = context['execution_date']
    exception = context.get('exception', 'Unknown error')
    
    subject = f"❌ ETL Failed: {dag_id} - {task_id}"
    body = f"""
    <h2>ETL Pipeline Failed</h2>
    <table border="1" cellpadding="5">
        <tr><td><b>DAG</b></td><td>{dag_id}</td></tr>
        <tr><td><b>Task</b></td><td>{task_id}</td></tr>
        <tr><td><b>Error</b></td><td>{exception}</td></tr>
    </table>
    """
    send_email(to=ALERT_EMAIL, subject=subject, html_content=body)


# Usage in DAG definition:
with DAG(
    dag_id='etl_customers',
    default_args={**DEFAULT_ARGS, 'on_failure_callback': send_failure_email},
    ...
):
    end = EmptyOperator(task_id='end', on_success_callback=send_success_email)
```

---

## T0028) Combined E-T-L Pipeline

```python
# ========================================
# Team 1 - T0028: Combined E-T-L Pipeline
# ========================================
# File: dags/etl_customers.py (example pattern for all table DAGs)
"""
etl_customers.py - Customers Table ETL Pipeline
Pipeline: Extract → Transform → Load (single DAG)
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


def extract_customers(**context):
    """Extract customers data from CSV"""
    import pandas as pd
    source_file = f'{DATA_RAW}/Customers.csv'
    staging_file = f'{DATA_STAGING}/customers_raw.csv'
    
    df = pd.read_csv(source_file)
    df.to_csv(staging_file, index=False)
    
    context['ti'].xcom_push(key='staging_file', value=staging_file)
    return {'rows': len(df), 'file': staging_file}


def transform_customers(**context):
    """Clean and transform customers data"""
    import pandas as pd
    staging_file = context['ti'].xcom_pull(key='staging_file', task_ids='extract')
    output_file = f'{DATA_PROCESSED}/customers_cleaned.csv'
    
    df = pd.read_csv(staging_file)
    
    # Data cleaning
    df = df.drop_duplicates(subset=['CustomerKey'], keep='first')
    df['Name'] = df['Name'].fillna('Unknown').str.strip()
    
    df.to_csv(output_file, index=False)
    context['ti'].xcom_push(key='output_file', value=output_file)
    return {'rows': len(df), 'file': output_file}


def load_customers(**context):
    """Load customers to PostgreSQL"""
    import pandas as pd
    from sqlalchemy import create_engine, text
    
    output_file = context['ti'].xcom_pull(key='output_file', task_ids='transform')
    df = pd.read_csv(output_file)
    
    engine = create_engine(get_connection_string())
    
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS etl_output"))
        conn.execute(text("DROP TABLE IF EXISTS etl_output.customers"))
    
    df.to_sql('customers', engine, schema='etl_output', if_exists='replace', index=False)
    return {'rows': len(df), 'table': 'etl_output.customers'}


with DAG(
    dag_id='etl_customers',
    default_args={**DEFAULT_ARGS, 'on_failure_callback': send_failure_email},
    start_date=START_DATE,
    schedule_interval=None,
    catchup=False,
    tags=['team1', 'etl', 'customers'],
) as dag:
    start = EmptyOperator(task_id='start')
    
    extract = PythonOperator(task_id='extract', python_callable=extract_customers)
    transform = PythonOperator(task_id='transform', python_callable=transform_customers)
    load = PythonOperator(task_id='load', python_callable=load_customers)
    
    end = EmptyOperator(task_id='end', on_success_callback=send_success_email)
    
    start >> extract >> transform >> load >> end
```

---

## T0029) Multi-Source Data Pipelines

```python
# ========================================
# Team 1 - T0029: Multi-Source Data Pipelines
# ========================================
# File: dags/dag_base.py - SOURCE_FILES configuration
# Files: dags/etl_customers.py, etl_products.py, etl_stores.py, 
#        etl_exchange_rates.py, etl_sales.py

# Each DAG follows the same E-T-L pattern:
# 1. etl_customers.py   → Customers.csv    → etl_output.customers
# 2. etl_products.py    → Products.csv     → etl_output.products
# 3. etl_stores.py      → Stores.csv       → etl_output.stores
# 4. etl_exchange_rates.py → Exchange_Rates.csv → etl_output.exchange_rates
# 5. etl_sales.py       → Sales.csv        → etl_output.sales

# Source files configuration from dag_base.py:
SOURCE_FILES = {
    'customers': f'{DATA_RAW}/Customers.csv',
    'sales': f'{DATA_RAW}/Sales.csv',
    'products': f'{DATA_RAW}/Products.csv',
    'stores': f'{DATA_RAW}/Stores.csv',
    'exchange_rates': f'{DATA_RAW}/Exchange_Rates.csv',
}

# Database output tables:
# - etl_output.customers    (15,266 rows)
# - etl_output.products     (2,517 rows)
# - etl_output.stores       (67 rows)
# - etl_output.exchange_rates (3,655 rows)
# - etl_output.sales        (26,326 rows)
```

---

## T0030) Reusable Pipeline Config

```python
# ========================================
# Team 1 - T0030: Reusable Pipeline Config
# ========================================
# File: dags/dag_base.py
"""
dag_base.py - Shared DAG Configuration and Utilities

Provides:
- Common DAG default arguments (retries, timeouts, email)
- Email notification settings (success/failure callbacks)
- Shared utility functions for all ETL DAGs
- Database connection utilities
- Path constants for data directories
"""
from datetime import datetime, timedelta

# ========================================
# DATA PATHS
# ========================================
DATA_RAW = '/opt/airflow/data/raw/dataset'
DATA_STAGING = '/opt/airflow/data/staging'
DATA_PROCESSED = '/opt/airflow/data/processed'

# ========================================
# DATABASE CONFIGURATION
# ========================================
DB_CONFIG = {
    'host': 'postgres',  # Docker service name
    'port': 5432,
    'database': 'airflow',
    'user': 'airflow',
    'password': 'airflow',
    'schema': 'etl_output'
}

def get_connection_string():
    """Get PostgreSQL connection string for ETL tables"""
    return (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )

# ========================================
# XCOM KEYS FOR INTER-DAG COMMUNICATION
# ========================================
XCOM_KEYS = {
    'extraction_complete': 'extraction_complete',
    'transformation_complete': 'transformation_complete',
    'load_complete': 'load_complete',
    'row_count': 'row_count',
    'output_file': 'output_file',
}
```

---

## T0031) Pipeline Execution Summary

```python
# ========================================
# Team 1 - T0031: Pipeline Execution Summary
# ========================================
# File: dags/etl_master_orchestrator.py

def generate_execution_summary(**context):
    """Generate comprehensive execution summary for all DAGs."""
    ti = context['ti']
    run_id = context['run_id']
    execution_date = context['execution_date']
    
    print("\n" + "=" * 70)
    print("📊 MASTER ORCHESTRATOR - EXECUTION SUMMARY")
    print("=" * 70)
    
    # Collect timing for each stage
    stages = ['Stage1_Independent', 'Stage2_Sales', 'Stage3_Reports']
    total_duration = 0
    stage_results = []
    
    for stage in stages:
        duration = ti.xcom_pull(key=f'{stage}_duration') or 0
        total_duration += duration
        stage_results.append({
            'stage': stage,
            'duration_seconds': duration,
        })
    
    print(f"\n⏱️  TOTAL PIPELINE DURATION: {total_duration:.2f} seconds")
    
    # Get status of all child DAGs
    all_dags = INDEPENDENT_DAGS + DEPENDENT_DAGS + FINAL_DAGS
    dag_statuses = []
    
    for dag_id in all_dags:
        dag_runs = DagRun.find(dag_id=dag_id, state=DagRunState.SUCCESS)
        if dag_runs:
            latest = max(dag_runs, key=lambda x: x.execution_date)
            status = 'SUCCESS'
        else:
            status = 'NO RUNS'
        dag_statuses.append({'dag_id': dag_id, 'status': status})
    
    # Save summary to JSON file
    summary = {
        'orchestrator_run_id': run_id,
        'execution_date': str(execution_date),
        'total_duration_seconds': total_duration,
        'stages': stage_results,
        'dag_statuses': dag_statuses,
        'generated_at': datetime.now().isoformat()
    }
    
    reports_dir = os.path.join(DATA_PROCESSED, 'reports')
    os.makedirs(reports_dir, exist_ok=True)
    
    summary_file = os.path.join(reports_dir, 'orchestrator_execution_summary.json')
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2, default=str)
    
    print(f"\n💾 Summary saved to: {summary_file}")
    return summary
```

Also generates 9 CSV reports in etl_reports.py:
```python
# File: dags/etl_reports.py - generate_all_reports()
# Reports generated:
# 1. customer_demographics.csv    - Customer count by country
# 2. product_catalog_summary.csv  - Products by category with price stats
# 3. store_performance.csv        - Store sales performance metrics
# 4. sales_by_date.csv           - Daily sales aggregation
# 5. top_products.csv            - Top 50 products by quantity sold
# 6. customer_orders.csv         - Customer order frequency
# 7. exchange_rate_analysis.csv  - Currency conversion analysis
# 8. sales_by_store_category.csv - Store-category cross analysis
# 9. pipeline_summary.csv        - Overall ETL pipeline statistics
```

---

## T0032) Error Recovery Workflow

```python
# ========================================
# Team 1 - T0032: Error Recovery Workflow
# ========================================
# File: dags/dag_base.py

# Retry configuration in DEFAULT_ARGS
DEFAULT_ARGS = {
    'retries': 3,                              # Retry failed tasks 3 times
    'retry_delay': timedelta(minutes=1),       # Wait 1 minute between retries
    'execution_timeout': timedelta(hours=2),   # Max task runtime
    'on_failure_callback': send_failure_email, # Alert on final failure
}

# Automatic recovery via catchup
DAG_CONFIG = {
    'catchup': True,          # Re-run missed intervals automatically
    'max_active_runs': 3,     # Limit concurrent recovery runs
}

# ExternalTaskSensor with failure handling
wait_for_products = ExternalTaskSensor(
    task_id='wait_for_products',
    external_dag_id='etl_products',
    external_task_id='end',
    allowed_states=['success'],
    failed_states=['failed', 'skipped'],  # Fail fast if upstream failed
    execution_date_fn=lambda dt: get_latest_execution_date(dt, 'etl_products'),
    timeout=7200,       # Timeout after 2 hours of waiting
    mode='reschedule',  # Free worker slot while waiting
)

# Manual recovery commands:
# 1. Clear failed task to retry:
#    airflow tasks clear etl_sales -t load -s 2025-12-26 -e 2025-12-26
# 
# 2. Re-run specific DAG run:
#    airflow dags trigger etl_sales
#
# 3. Mark task as success to skip:
#    airflow tasks set-state etl_sales load success -d 2025-12-26
```

---

### Cross-References
- **Master Orchestrator:** dags/etl_master_orchestrator.py (TaskGroups, TriggerDagRunOperator)
- **Dimension DAGs:** dags/etl_customers.py, etl_products.py, etl_stores.py, etl_exchange_rates.py
- **Fact DAG:** dags/etl_sales.py (ExternalTaskSensor for etl_products)
- **Reports DAG:** dags/etl_reports.py (5 ExternalTaskSensors, 9 CSV reports)
- **Shared Config:** dags/dag_base.py (DEFAULT_ARGS, callbacks, paths, DB config)
- **Utility Modules:** scripts/utils/*.py (validation, aggregation, bulk load, upsert, etc.)

### Summary
All 32 tasks (T0007–T0032) are fully implemented across 6 sprints:

**Sprint 2 - Data Quality (T0008–T0012):** Cleaning utilities, type handling, deduplication, missing value strategies, config-driven rules.

**Sprint 3 - Transformations (T0013–T0017):** Aggregations, normalization, feature engineering, date/time transformations, config-based rules.

**Sprint 4 - Loading (T0018–T0022):** Bulk operations with chunking, incremental/full loads, constraint handling, upsert logic, reject table creation.

**Sprint 5 - Orchestration (T0023–T0027):** Master DAG with TaskGroups, ExternalTaskSensor for event-driven triggering, multi-DAG dependencies, backfill/catchup, failure handling with retries and email callbacks.

**Sprint 6 - Combined Pipeline (T0028–T0032):** 5 source-specific E-T-L DAGs, reusable dag_base.py config, execution summary generation (JSON + 9 CSV reports), error recovery workflow.

**Database Tables Created:**
| Table | Rows | Source |
|-------|------|--------|
| etl_output.customers | 15,266 | Customers.csv |
| etl_output.products | 2,517 | Products.csv |
| etl_output.stores | 67 | Stores.csv |
| etl_output.exchange_rates | 3,655 | Exchange_Rates.csv |
| etl_output.sales | 26,326 | Sales.csv |

Reusable utility modules in `scripts/utils/` provide modular components for validation, aggregation, bulk loading, upserts, and rejected record handling.
