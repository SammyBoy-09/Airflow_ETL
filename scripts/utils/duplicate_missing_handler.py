############# T0010: Duplicate Data Detection & Removal #############
############# T0011: Missing Data Handling Strategies #############

"""
Duplicate & Missing Data Handlers
Part of T0010: Duplicate Data Detection & Removal
Part of T0011: Missing Data Handling Strategies

DuplicateHandler provides:
- Duplicate detection on single or multiple key columns
- Duplicate removal (keep first/last/all options)
- Duplicate statistics reporting

MissingDataHandler provides:
- Missing data analysis and statistics
- Row dropping (with threshold)
- Column dropping (with threshold)
- Per-column missing value fill strategies
"""

import pandas as pd
import numpy as np
from typing import Tuple, List, Dict, Any
import logging

logger = logging.getLogger(__name__)


class DuplicateHandler:
    """Handle duplicate records in DataFrames"""
    
    @staticmethod
    def detect_duplicates(df: pd.DataFrame, subset: List[str] = None, 
                         keep: str = 'first') -> Tuple[pd.DataFrame, pd.DataFrame, int]:
        """
        Detect and separate duplicate rows
        
        Args:
            df: Input DataFrame
            subset: Columns to consider for duplication (None = all)
            keep: 'first', 'last', False (mark all)
        
        Returns:
            (dataframe_without_dupes, duplicate_rows, count_removed)
        """
        if subset and not all(col in df.columns for col in subset):
            missing = [col for col in subset if col not in df.columns]
            logger.error(f"❌ Missing columns: {missing}")
            return df, pd.DataFrame(), 0
        
        # Find all duplicates
        dup_mask = df.duplicated(subset=subset, keep=False)
        duplicate_rows = df[dup_mask]
        
        # Remove duplicates keeping only first/last occurrence
        cleaned_df = df.drop_duplicates(subset=subset, keep=keep)
        
        count_removed = len(df) - len(cleaned_df)
        
        logger.info(f"✅ Removed {count_removed} duplicate rows (kept: {keep})")
        
        return cleaned_df, duplicate_rows, count_removed
    
    @staticmethod
    def remove_duplicates(df: pd.DataFrame, subset: List[str] = None, 
                         keep: str = 'first', inplace: bool = False) -> pd.DataFrame:
        """
        Remove duplicate rows from DataFrame
        
        Returns:
            DataFrame with duplicates removed
        """
        cleaned_df, _, count = DuplicateHandler.detect_duplicates(df, subset, keep)
        return cleaned_df
    
    @staticmethod
    def report_duplicates(df: pd.DataFrame, subset: List[str] = None) -> Dict[str, Any]:
        """
        Generate duplicate report
        
        Returns:
            Dictionary with duplicate statistics
        """
        dup_mask = df.duplicated(subset=subset, keep=False)
        dup_count = dup_mask.sum()
        
        dup_rows = df[dup_mask]
        
        # Find most common duplicates
        if len(dup_rows) > 0:
            dup_groups = dup_rows.duplicated(subset=subset, keep=False)
            most_common = dup_rows[subset].value_counts().head(5)
        else:
            most_common = {}
        
        return {
            'total_rows': len(df),
            'duplicate_rows': int(dup_count),
            'unique_rows': len(df) - int(dup_count),
            'duplicate_percent': round((dup_count / len(df)) * 100, 2),
            'key_columns': subset,
            'most_common_duplicates': most_common.to_dict() if len(most_common) > 0 else {}
        }


class MissingDataHandler:
    """Handle missing/null values in DataFrames"""
    
    @staticmethod
    def analyze_missing(df: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze missing data patterns
        
        Returns:
            Dictionary with missing data statistics
        """
        missing_stats = {}
        
        for col in df.columns:
            null_count = df[col].isnull().sum()
            null_pct = (null_count / len(df)) * 100
            
            if null_count > 0:
                missing_stats[col] = {
                    'null_count': int(null_count),
                    'null_percent': round(null_pct, 2),
                    'data_type': str(df[col].dtype)
                }
        
        total_missing = sum(df.isnull().sum())
        
        return {
            'columns_with_missing': len(missing_stats),
            'total_missing_values': int(total_missing),
            'total_cells': len(df) * len(df.columns),
            'missing_percent': round((total_missing / (len(df) * len(df.columns))) * 100, 2),
            'column_details': missing_stats
        }
    
    @staticmethod
    def drop_rows_with_nulls(df: pd.DataFrame, subset: List[str] = None, 
                            how: str = 'any') -> Tuple[pd.DataFrame, int]:
        """
        Drop rows with null values
        
        Args:
            df: Input DataFrame
            subset: Columns to check (None = all)
            how: 'any' (drop if any null), 'all' (drop if all null)
        
        Returns:
            (cleaned_df, rows_dropped)
        """
        initial_rows = len(df)
        cleaned_df = df.dropna(subset=subset, how=how)
        rows_dropped = initial_rows - len(cleaned_df)
        
        logger.info(f"✅ Dropped {rows_dropped} rows with {how} nulls")
        return cleaned_df, rows_dropped
    
    @staticmethod
    def drop_columns_with_nulls(df: pd.DataFrame, threshold: float = 0.5) -> Tuple[pd.DataFrame, List[str]]:
        """
        Drop columns where >threshold of values are null
        
        Args:
            threshold: Drop columns where null% > threshold (0-1)
        
        Returns:
            (cleaned_df, dropped_columns)
        """
        dropped = []
        cols_to_drop = []
        
        for col in df.columns:
            null_pct = df[col].isnull().sum() / len(df)
            if null_pct > threshold:
                cols_to_drop.append(col)
                dropped.append({'column': col, 'null_percent': round(null_pct * 100, 2)})
        
        cleaned_df = df.drop(columns=cols_to_drop)
        
        logger.info(f"✅ Dropped {len(cols_to_drop)} columns with >{threshold*100}% nulls")
        return cleaned_df, dropped
    
    @staticmethod
    def fill_by_strategy(df: pd.DataFrame, strategy_map: Dict[str, Dict[str, Any]]) -> Tuple[pd.DataFrame, Dict[str, int]]:
        """
        Fill missing values using per-column strategies
        
        Args:
            df: Input DataFrame
            strategy_map: {column: {strategy: 'mean'/'median'/'mode'/'forward'/'fill_value', 'value': X}}
        
        Returns:
            (filled_df, fill_counts)
        """
        from .transformation_utils import DataTransformer
        
        filled_df = df.copy()
        fill_counts = {}
        
        for col, config in strategy_map.items():
            if col not in filled_df.columns:
                logger.warning(f"⚠️ Column not found: {col}")
                continue
            
            strategy = config.get('strategy')
            
            try:
                if strategy == 'mean':
                    filled_df[col], count = DataTransformer.fill_missing_mean(filled_df[col])
                    fill_counts[col] = count
                
                elif strategy == 'median':
                    filled_df[col], count = DataTransformer.fill_missing_median(filled_df[col])
                    fill_counts[col] = count
                
                elif strategy == 'mode':
                    filled_df[col], count = DataTransformer.fill_missing_mode(filled_df[col])
                    fill_counts[col] = count
                
                elif strategy == 'forward':
                    filled_df[col], count = DataTransformer.fill_missing_forward(filled_df[col])
                    fill_counts[col] = count
                
                elif strategy == 'backward':
                    filled_df[col], count = DataTransformer.fill_missing_backward(filled_df[col])
                    fill_counts[col] = count
                
                elif strategy == 'fill_value':
                    fill_value = config.get('value')
                    filled_df[col], count = DataTransformer.fill_missing_custom(filled_df[col], fill_value)
                    fill_counts[col] = count
                
                else:
                    logger.warning(f"⚠️ Unknown strategy: {strategy}")
            
            except Exception as e:
                logger.error(f"❌ Error filling {col}: {e}")
        
        return filled_df, fill_counts
