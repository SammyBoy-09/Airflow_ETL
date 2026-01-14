############# T0008: Build Reusable Cleaning Utilities #############
############# Transformation Component #############
############# T0009: Handle Incorrect Data Types #############

"""
Transformation Utilities - Data Type Conversion & Standardization
Part of T0008: Build Reusable Cleaning Utilities
Part of T0009: Handle Incorrect Data Types

Provides:
- Safe type casting (int, float, string, date, bool)
- Date format standardization (multiple input formats → single output)
- Missing value imputation (6 strategies: mean, median, mode, forward, backward, custom)
- Text normalization (trim, case conversion)
- Email cleaning and validation
- Special character removal
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, List, Callable, Tuple
from datetime import datetime
import logging
from dateutil.parser import parse as parse_date

logger = logging.getLogger(__name__)


class DataTransformer:
    """Data transformation and type conversion utilities"""
    
    @staticmethod
    def safe_typecast(series: pd.Series, target_type: str, on_error: str = 'keep') -> pd.Series:
        """
        Safely convert series to target type
        
        Args:
            series: Input series
            target_type: 'int', 'float', 'string', 'date', 'bool'
            on_error: 'keep' (original), 'none' (null), 'reject' (mark)
        
        Returns:
            Converted series
        """
        series = series.copy()
        
        try:
            if target_type == 'int':
                # Convert via float first to handle decimals
                converted = pd.to_numeric(series, errors='coerce').astype('Int64')
                if on_error == 'keep':
                    # Keep originals where conversion failed
                    mask = converted.isna() & series.notna()
                    series = converted
                    series[mask] = pd.NA
                else:
                    series = converted
            
            elif target_type == 'float':
                series = pd.to_numeric(series, errors='coerce').astype('float64')
            
            elif target_type == 'string':
                series = series.astype(str)
            
            elif target_type == 'date':
                series = pd.to_datetime(series, errors='coerce')
            
            elif target_type == 'bool':
                series = series.astype(bool)
            
            logger.info(f"✅ Converted {series.name} to {target_type}")
            return series
        
        except Exception as e:
            logger.error(f"❌ Error converting {series.name} to {target_type}: {e}")
            return series
    
    @staticmethod
    def standardize_date_format(series: pd.Series, input_formats: List[str], 
                                output_format: str = '%d-%m-%Y') -> Tuple[pd.Series, List[int]]:
        """
        Parse dates in multiple formats and standardize to one format
        
        Args:
            series: Input date series (as strings)
            input_formats: List of possible input formats
            output_format: Target format (default DD-MM-YYYY)
        
        Returns:
            (converted_series, indices_of_failed_conversions)
        """
        series = series.copy()
        failed_indices = []
        converted = []
        
        for idx, val in series.items():
            if pd.isna(val):
                converted.append(None)
                continue
            
            val_str = str(val).strip()
            parsed = None
            
            # Try each format
            for fmt in input_formats:
                try:
                    parsed = datetime.strptime(val_str, fmt)
                    break
                except (ValueError, TypeError):
                    continue
            
            # If no format worked, try fuzzy parsing
            if parsed is None:
                try:
                    parsed = parse_date(val_str)
                except:
                    failed_indices.append(idx)
                    converted.append(None)
                    continue
            
            # Format to output
            try:
                converted.append(parsed.strftime(output_format))
            except:
                failed_indices.append(idx)
                converted.append(None)
        
        result_series = pd.Series(converted, index=series.index)
        logger.info(f"✅ Standardized dates: {len(series) - len(failed_indices)} success, {len(failed_indices)} failed")
        
        return result_series, failed_indices
    
    @staticmethod
    def fill_missing_mean(series: pd.Series) -> Tuple[pd.Series, int]:
        """
        Fill missing numeric values with column mean
        
        Returns:
            (filled_series, count_filled)
        """
        series = series.copy()
        
        # Convert to numeric
        numeric_series = pd.to_numeric(series, errors='coerce')
        mean_val = numeric_series.mean()
        
        count_filled = 0
        for idx in series.index:
            if pd.isna(series[idx]):
                series[idx] = mean_val
                count_filled += 1
        
        logger.info(f"✅ Filled {count_filled} missing values with mean ({mean_val:.2f})")
        return series, count_filled
    
    @staticmethod
    def fill_missing_median(series: pd.Series) -> Tuple[pd.Series, int]:
        """Fill missing numeric values with median"""
        series = series.copy()
        numeric_series = pd.to_numeric(series, errors='coerce')
        median_val = numeric_series.median()
        
        count_filled = 0
        for idx in series.index:
            if pd.isna(series[idx]):
                series[idx] = median_val
                count_filled += 1
        
        logger.info(f"✅ Filled {count_filled} missing values with median ({median_val:.2f})")
        return series, count_filled
    
    @staticmethod
    def fill_missing_mode(series: pd.Series) -> Tuple[pd.Series, int]:
        """Fill missing values with mode (most common value)"""
        series = series.copy()
        mode_val = series.mode()
        
        if len(mode_val) == 0:
            logger.warning(f"⚠️ No mode found for {series.name}")
            return series, 0
        
        mode_val = mode_val[0]
        count_filled = 0
        
        for idx in series.index:
            if pd.isna(series[idx]):
                series[idx] = mode_val
                count_filled += 1
        
        logger.info(f"✅ Filled {count_filled} missing values with mode ({mode_val})")
        return series, count_filled
    
    @staticmethod
    def fill_missing_custom(series: pd.Series, fill_value: Any) -> Tuple[pd.Series, int]:
        """Fill missing values with custom value"""
        series = series.copy()
        
        count_filled = series.isna().sum()
        series = series.fillna(fill_value)
        
        logger.info(f"✅ Filled {count_filled} missing values with '{fill_value}'")
        return series, int(count_filled)
    
    @staticmethod
    def fill_missing_forward(series: pd.Series) -> Tuple[pd.Series, int]:
        """Forward fill missing values (use previous valid value)"""
        series = series.copy()
        count_filled = series.isna().sum()
        series = series.fillna(method='ffill')
        
        logger.info(f"✅ Forward filled {count_filled} missing values")
        return series, int(count_filled)
    
    @staticmethod
    def fill_missing_backward(series: pd.Series) -> Tuple[pd.Series, int]:
        """Backward fill missing values (use next valid value)"""
        series = series.copy()
        count_filled = series.isna().sum()
        series = series.fillna(method='bfill')
        
        logger.info(f"✅ Backward filled {count_filled} missing values")
        return series, int(count_filled)
    
    @staticmethod
    def normalize_text(series: pd.Series, action: str = 'trim') -> Tuple[pd.Series, int]:
        """
        Normalize text columns
        
        Args:
            series: Input series
            action: 'trim' (whitespace), 'lower', 'upper', 'title'
        
        Returns:
            (normalized_series, count_changed)
        """
        series = series.copy()
        count_changed = 0
        
        if action == 'trim':
            for idx in series.index:
                if isinstance(series[idx], str):
                    trimmed = series[idx].strip()
                    if trimmed != series[idx]:
                        count_changed += 1
                    series[idx] = trimmed
        
        elif action == 'lower':
            for idx in series.index:
                if isinstance(series[idx], str) and series[idx].lower() != series[idx]:
                    count_changed += 1
                series[idx] = series[idx].lower() if isinstance(series[idx], str) else series[idx]
        
        elif action == 'upper':
            for idx in series.index:
                if isinstance(series[idx], str) and series[idx].upper() != series[idx]:
                    count_changed += 1
                series[idx] = series[idx].upper() if isinstance(series[idx], str) else series[idx]
        
        elif action == 'title':
            for idx in series.index:
                if isinstance(series[idx], str) and series[idx].title() != series[idx]:
                    count_changed += 1
                series[idx] = series[idx].title() if isinstance(series[idx], str) else series[idx]
        
        logger.info(f"✅ Applied {action} normalization: {count_changed} values changed")
        return series, count_changed
    
    @staticmethod
    def clean_email(series: pd.Series, invalid_replacement: str = 'invalid@company.com') -> Tuple[pd.Series, List[int]]:
        """
        Clean and standardize email addresses
        
        Returns:
            (cleaned_series, invalid_indices)
        """
        import re
        
        series = series.copy()
        invalid_indices = []
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        
        for idx, val in series.items():
            if pd.isna(val):
                continue
            
            email = str(val).strip().lower()
            
            if not re.match(pattern, email):
                invalid_indices.append(idx)
                series[idx] = invalid_replacement
            else:
                series[idx] = email
        
        logger.info(f"✅ Cleaned emails: {len(series) - len(invalid_indices)} valid, {len(invalid_indices)} invalid")
        return series, invalid_indices
    
    @staticmethod
    def remove_special_chars(series: pd.Series, keep_chars: str = '') -> Tuple[pd.Series, int]:
        """
        Remove special characters from text
        
        Args:
            series: Input series
            keep_chars: Characters to keep (e.g., '-' for keeping hyphens)
        
        Returns:
            (cleaned_series, count_changed)
        """
        import re
        
        series = series.copy()
        count_changed = 0
        
        # Build pattern: keep alphanumeric, spaces, and specified chars
        pattern = f'[^a-zA-Z0-9\\s{re.escape(keep_chars)}]'
        
        for idx, val in series.items():
            if isinstance(val, str):
                cleaned = re.sub(pattern, '', val)
                if cleaned != val:
                    count_changed += 1
                series[idx] = cleaned
        
        logger.info(f"✅ Removed special characters: {count_changed} values changed")
        return series, count_changed
