############# T0012: Config-Driven Cleaning Rules #############

"""
Config-Driven Cleaning Engine - Orchestrates All Data Quality Utilities
Part of T0012: Config-Driven Cleaning Rules

ConfigDrivenCleaner provides:
- YAML configuration loading and parsing
- Dynamic cleaning rule application
- Complete cleaning pipeline orchestration:
  1. Duplicate handling (T0010)
  2. Type conversions (T0009)
  3. Missing value imputation (T0011)
  4. Date standardization (T0008)
  5. Validation & cleaning (T0008)
  6. Column selection/dropping
  7. Quality checks
- Database error logging integration
- Detailed cleaning reports
"""

import pandas as pd
from typing import Dict, Any, Tuple, List, Optional
import logging
from pathlib import Path
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config_loader import ConfigLoader
from .validation_utils import DataValidator
from .transformation_utils import DataTransformer
from .duplicate_missing_handler import DuplicateHandler, MissingDataHandler
from .db_utils import DatabaseManager

logger = logging.getLogger(__name__)


class ConfigDrivenCleaner:
    """
    Apply YAML-based cleaning rules to any dataset
    Supports duplicates, type conversion, missing values, and validation
    """
    
    def __init__(self, config_path: str, db_manager: Optional[DatabaseManager] = None):
        """
        Initialize cleaner with config file
        
        Args:
            config_path: Path to YAML config file
            db_manager: Optional DatabaseManager for logging rejections
        """
        self.config = ConfigLoader.load_yaml(config_path)
        self.db_manager = db_manager
        self.config_path = config_path
        self.source_name = Path(config_path).stem  # e.g., 'customers' from customers_config.yaml
        
        logger.info(f"✅ Loaded config: {config_path}")
    
    def load_data(self, csv_path: Optional[str] = None) -> pd.DataFrame:
        """
        Load data from CSV or database
        
        Args:
            csv_path: Optional CSV file path (overrides config)
        
        Returns:
            Loaded DataFrame
        """
        if csv_path:
            df = pd.read_csv(csv_path)
            logger.info(f"✅ Loaded CSV: {csv_path} ({len(df)} rows)")
            return df
        
        # Use CSV from config
        csv_file = self.config.get('source', {}).get('csv_file')
        if csv_file:
            df = pd.read_csv(csv_file)
            logger.info(f"✅ Loaded CSV: {csv_file} ({len(df)} rows)")
            return df
        
        logger.error("❌ No CSV file specified")
        return None
    
    def clean(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Apply all cleaning rules to DataFrame
        
        Returns:
            (cleaned_df, report)
        """
        report = {
            'source': self.source_name,
            'initial_rows': len(df),
            'initial_columns': len(df.columns),
            'steps': []
        }
        
        # 1. Handle duplicates
        if self.config.get('cleaning_rules', {}).get('duplicates', {}).get('enabled'):
            df, dup_report = self._handle_duplicates(df)
            report['steps'].append(dup_report)
        
        # 2. Type conversions
        if 'type_conversions' in self.config.get('cleaning_rules', {}):
            df, type_report = self._handle_type_conversions(df)
            report['steps'].append(type_report)
        
        # 3. Handle missing values
        if 'missing_values' in self.config.get('cleaning_rules', {}):
            df, missing_report = self._handle_missing_values(df)
            report['steps'].append(missing_report)
        
        # 4. Standardize formats (dates, text)
        if 'date_columns' in self.config.get('cleaning_rules', {}):
            df, date_report = self._handle_date_formats(df)
            report['steps'].append(date_report)
        
        # 5. Validate and clean (emails, special chars, etc.)
        if 'validations' in self.config.get('cleaning_rules', {}):
            df, valid_report = self._handle_validations(df)
            report['steps'].append(valid_report)
        
        # 6. Select and drop columns
        df = self._select_columns(df)
        
        # 7. Run quality checks
        if 'quality_checks' in self.config:
            checks = self.config['quality_checks']
            check_results = DataValidator.run_quality_checks(df, checks)
            report['steps'].append({
                'step': 'quality_checks',
                'details': check_results
            })
        
        report['final_rows'] = len(df)
        report['final_columns'] = len(df.columns)
        report['rows_removed'] = report['initial_rows'] - report['final_rows']
        
        logger.info(f"✅ Cleaning complete: {report['rows_removed']} rows removed, "
                   f"{report['initial_columns'] - report['final_columns']} columns removed")
        
        return df, report
    
    def _handle_duplicates(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Handle duplicate records"""
        dup_config = self.config['cleaning_rules'].get('duplicates', {})
        
        subset = dup_config.get('subset')
        keep = dup_config.get('keep', 'first')
        
        cleaned_df, dup_rows, count = DuplicateHandler.detect_duplicates(df, subset, keep)
        
        # Log duplicates if database available
        if self.db_manager and not dup_rows.empty:
            for idx, row in dup_rows.iterrows():
                if dup_config.get('log_to_rejects'):
                    key_val = '-'.join(str(row[col]) for col in (subset or []))
                    self.db_manager.log_rejection(
                        source_table=self.source_name,
                        record_id=str(key_val),
                        error_type='duplicate',
                        error_details=f"Duplicate on columns: {subset}",
                        rejected_data=row.to_dict()
                    )
        
        report = dup_config.copy()
        report.pop('enabled', None)
        report.update({
            'step': 'handle_duplicates',
            'duplicates_found': count,
            'duplicates_kept': keep
        })
        
        return cleaned_df, report
    
    def _handle_type_conversions(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Convert column types"""
        conversions = self.config['cleaning_rules'].get('type_conversions', {})
        
        report = {
            'step': 'type_conversions',
            'conversions': {}
        }
        
        for col, config in conversions.items():
            if col not in df.columns:
                logger.warning(f"⚠️ Column not found: {col}")
                continue
            
            target_type = config.get('target_type')
            df[col] = DataTransformer.safe_typecast(df[col], target_type, config.get('handle_errors', 'keep'))
            
            report['conversions'][col] = {
                'target_type': target_type,
                'handle_errors': config.get('handle_errors', 'keep')
            }
        
        return df, report
    
    def _handle_missing_values(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Fill missing values"""
        missing_config = self.config['cleaning_rules'].get('missing_values', {})
        
        strategy_map = {}
        for col, config in missing_config.items():
            if col in df.columns:
                strategy_map[col] = config
        
        filled_df, fill_counts = MissingDataHandler.fill_by_strategy(df, strategy_map)
        
        report = {
            'step': 'missing_values',
            'fill_counts': fill_counts
        }
        
        return filled_df, report
    
    def _handle_date_formats(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Standardize date formats"""
        date_columns = self.config['cleaning_rules'].get('date_columns', {})
        
        report = {
            'step': 'date_formats',
            'conversions': {}
        }
        
        for col, config in date_columns.items():
            if col not in df.columns:
                logger.warning(f"⚠️ Column not found: {col}")
                continue
            
            input_formats = config.get('input_formats', ['%Y-%m-%d'])
            output_format = config.get('output_format', '%d-%m-%Y')
            
            df[col], failed_indices = DataTransformer.standardize_date_format(
                df[col],
                input_formats,
                output_format
            )
            
            # Log failures
            if failed_indices and self.db_manager:
                for idx in failed_indices[:100]:  # Log first 100
                    if idx in df.index:
                        self.db_manager.log_rejection(
                            source_table=self.source_name,
                            record_id=str(idx),
                            error_type='date_conversion',
                            error_details=f"Failed to parse date in {col}",
                            rejected_data={'column': col, 'value': str(df.loc[idx, col])}
                        )
            
            report['conversions'][col] = {
                'input_formats': input_formats,
                'output_format': output_format,
                'failed_conversions': len(failed_indices)
            }
        
        return df, report
    
    def _handle_validations(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Apply validations and clean data"""
        validations = self.config['cleaning_rules'].get('validations', {})
        
        report = {
            'step': 'validations',
            'actions': {}
        }
        
        for col, config in validations.items():
            if col not in df.columns:
                logger.warning(f"⚠️ Column not found: {col}")
                continue
            
            if config.get('pattern'):  # Email validation
                df[col], invalid_indices = DataTransformer.clean_email(
                    df[col],
                    config.get('replacement_value', 'invalid@company.com')
                )
                report['actions'][col] = {
                    'type': 'email_validation',
                    'invalid_count': len(invalid_indices),
                    'replacement': config.get('replacement_value')
                }
        
        return df, report
    
    def _select_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Select/drop columns based on config"""
        to_select = self.config.get('columns_to_select')
        to_drop = self.config.get('columns_to_drop', [])
        
        if to_select:
            available_cols = [col for col in to_select if col in df.columns]
            df = df[available_cols]
            logger.info(f"✅ Selected {len(available_cols)} columns")
        
        if to_drop:
            df = df.drop(columns=[col for col in to_drop if col in df.columns])
            logger.info(f"✅ Dropped {len(to_drop)} columns")
        
        return df
    
    def save_cleaned_data(self, df: pd.DataFrame, output_path: str) -> bool:
        """
        Save cleaned data to CSV
        
        Args:
            df: Cleaned DataFrame
            output_path: Output file path
        
        Returns:
            True if successful
        """
        try:
            df.to_csv(output_path, index=False, encoding='utf-8')
            logger.info(f"✅ Saved cleaned data: {output_path}")
            return True
        except Exception as e:
            logger.error(f"❌ Error saving data: {e}")
            return False
    
    def run_complete_pipeline(self, csv_path: str, output_path: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Complete cleaning pipeline: load -> clean -> save -> report
        
        Returns:
            (cleaned_df, report)
        """
        # Load
        df = self.load_data(csv_path)
        if df is None:
            return None, {'error': 'Failed to load data'}
        
        # Clean
        cleaned_df, report = self.clean(df)
        
        # Save
        self.save_cleaned_data(cleaned_df, output_path)
        
        return cleaned_df, report
