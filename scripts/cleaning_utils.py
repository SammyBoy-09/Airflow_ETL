"""
Data Cleaning Utilities
Reusable functions for data cleaning and transformation
"""

import pandas as pd
import numpy as np
from typing import List, Union, Optional, Dict, Any
import re
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LinearRegression


class DataCleaner:
    """Comprehensive data cleaning utilities"""
    
    @staticmethod
    def trim_whitespace(df: pd.DataFrame, columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Trim leading and trailing whitespace from string columns
        
        Args:
            df: Input DataFrame
            columns: Specific columns to trim (None = all string columns)
            
        Returns:
            DataFrame with trimmed strings
        """
        df = df.copy()
        
        if columns is None:
            columns = df.select_dtypes(include=['object']).columns.tolist()
        
        for col in columns:
            if col in df.columns and df[col].dtype == 'object':
                df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
        
        return df
    
    @staticmethod
    def fill_missing_mean(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """
        Fill missing values with mean for numeric columns
        
        Args:
            df: Input DataFrame
            columns: Columns to fill
            
        Returns:
            DataFrame with filled values
        """
        df = df.copy()
        
        for col in columns:
            if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                mean_value = df[col].mean()
                df[col] = df[col].fillna(mean_value)
        
        return df
    
    @staticmethod
    def fill_missing_median(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """
        Fill missing values with median for numeric columns
        
        Args:
            df: Input DataFrame
            columns: Columns to fill
            
        Returns:
            DataFrame with filled values
        """
        df = df.copy()
        
        for col in columns:
            if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                median_value = df[col].median()
                df[col] = df[col].fillna(median_value)
        
        return df
    
    @staticmethod
    def fill_missing_mode(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """
        Fill missing values with mode (most frequent value)
        
        Args:
            df: Input DataFrame
            columns: Columns to fill
            
        Returns:
            DataFrame with filled values
        """
        df = df.copy()
        
        for col in columns:
            if col in df.columns:
                mode_value = df[col].mode()
                if len(mode_value) > 0:
                    df[col] = df[col].fillna(mode_value[0])
        
        return df
    
    @staticmethod
    def fill_missing_forward(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """
        Forward fill missing values (use previous value)
        
        Args:
            df: Input DataFrame
            columns: Columns to fill
            
        Returns:
            DataFrame with filled values
        """
        df = df.copy()
        
        for col in columns:
            if col in df.columns:
                df[col] = df[col].ffill()
        
        return df
    
    @staticmethod
    def fill_missing_backward(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """
        Backward fill missing values (use next value)
        
        Args:
            df: Input DataFrame
            columns: Columns to fill
            
        Returns:
            DataFrame with filled values
        """
        df = df.copy()
        
        for col in columns:
            if col in df.columns:
                df[col] = df[col].bfill()
        
        return df
    
    @staticmethod
    def fill_missing_regression(df: pd.DataFrame, target_col: str, predictor_cols: List[str]) -> pd.DataFrame:
        """
        Fill missing values using linear regression
        
        Args:
            df: Input DataFrame
            target_col: Column with missing values to fill
            predictor_cols: Columns to use as predictors
            
        Returns:
            DataFrame with filled values
        """
        df = df.copy()
        
        # Separate rows with and without missing values in target
        df_complete = df[df[target_col].notna()]
        df_missing = df[df[target_col].isna()]
        
        if len(df_missing) == 0 or len(df_complete) == 0:
            return df
        
        # Prepare training data
        X_train = df_complete[predictor_cols].select_dtypes(include=[np.number])
        y_train = df_complete[target_col]
        X_predict = df_missing[predictor_cols].select_dtypes(include=[np.number])
        
        # Train model and predict
        model = LinearRegression()
        model.fit(X_train, y_train)
        predictions = model.predict(X_predict)
        
        # Fill missing values
        df.loc[df[target_col].isna(), target_col] = predictions
        
        return df
    
    @staticmethod
    def remove_duplicates(df: pd.DataFrame, subset: Optional[List[str]] = None, 
                         keep: str = 'first') -> pd.DataFrame:
        """
        Remove duplicate rows
        
        Args:
            df: Input DataFrame
            subset: Columns to consider for identifying duplicates
            keep: Which duplicates to keep ('first', 'last', False)
            
        Returns:
            DataFrame without duplicates
        """
        return df.drop_duplicates(subset=subset, keep=keep)
    
    @staticmethod
    def typecast_column(df: pd.DataFrame, column: str, target_type: str) -> pd.DataFrame:
        """
        Convert column to specified data type
        
        Args:
            df: Input DataFrame
            column: Column name
            target_type: Target type ('int', 'float', 'str', 'datetime', 'bool')
            
        Returns:
            DataFrame with converted column
        """
        df = df.copy()
        
        if column not in df.columns:
            return df
        
        try:
            if target_type == 'int':
                df[column] = pd.to_numeric(df[column], errors='coerce').astype('Int64')
            elif target_type == 'float':
                df[column] = pd.to_numeric(df[column], errors='coerce')
            elif target_type == 'str':
                df[column] = df[column].astype(str)
            elif target_type == 'datetime':
                df[column] = pd.to_datetime(df[column], errors='coerce')
            elif target_type == 'bool':
                df[column] = df[column].astype(bool)
            else:
                print(f"Warning: Unknown type '{target_type}' for column '{column}'")
        except Exception as e:
            print(f"Error converting column '{column}' to {target_type}: {e}")
        
        return df
    
    @staticmethod
    def handle_missing_data(df: pd.DataFrame, strategy: str = 'drop', 
                           columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Handle missing data with specified strategy
        
        Args:
            df: Input DataFrame
            strategy: Strategy to use ('drop', 'mean', 'median', 'mode', 'ffill', 'bfill')
            columns: Specific columns to apply strategy (None = all columns)
            
        Returns:
            DataFrame with handled missing data
        """
        df = df.copy()
        
        if columns is None:
            columns = df.columns.tolist()
        
        if strategy == 'drop':
            df = df.dropna(subset=columns)
        elif strategy == 'mean':
            df = DataCleaner.fill_missing_mean(df, columns)
        elif strategy == 'median':
            df = DataCleaner.fill_missing_median(df, columns)
        elif strategy == 'mode':
            df = DataCleaner.fill_missing_mode(df, columns)
        elif strategy == 'ffill':
            df = DataCleaner.fill_missing_forward(df, columns)
        elif strategy == 'bfill':
            df = DataCleaner.fill_missing_backward(df, columns)
        else:
            print(f"Warning: Unknown strategy '{strategy}'")
        
        return df
    
    @staticmethod
    def validate_email(df: pd.DataFrame, email_column: str, drop_invalid: bool = True) -> pd.DataFrame:
        """
        Validate email format
        
        Args:
            df: Input DataFrame
            email_column: Column containing emails
            drop_invalid: Whether to drop invalid emails
            
        Returns:
            DataFrame with validated emails
        """
        df = df.copy()
        
        if email_column not in df.columns:
            return df
        
        email_pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
        
        if drop_invalid:
            df = df[df[email_column].str.contains(email_pattern, regex=True, na=False)]
        else:
            df['email_valid'] = df[email_column].str.contains(email_pattern, regex=True, na=False)
        
        return df
    
    @staticmethod
    def remove_empty_strings(df: pd.DataFrame, columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Replace empty strings with NaN
        
        Args:
            df: Input DataFrame
            columns: Columns to process (None = all string columns)
            
        Returns:
            DataFrame with empty strings replaced
        """
        df = df.copy()
        
        if columns is None:
            columns = df.select_dtypes(include=['object']).columns.tolist()
        
        for col in columns:
            if col in df.columns:
                df[col] = df[col].replace(r'^\s*$', np.nan, regex=True)
        
        return df
    
    @staticmethod
    def detect_outliers_iqr(df: pd.DataFrame, column: str, factor: float = 1.5) -> pd.Series:
        """
        Detect outliers using IQR method
        
        Args:
            df: Input DataFrame
            column: Column to check for outliers
            factor: IQR multiplier (1.5 is standard)
            
        Returns:
            Boolean Series indicating outliers
        """
        Q1 = df[column].quantile(0.25)
        Q3 = df[column].quantile(0.75)
        IQR = Q3 - Q1
        
        lower_bound = Q1 - factor * IQR
        upper_bound = Q3 + factor * IQR
        
        return (df[column] < lower_bound) | (df[column] > upper_bound)
    
    @staticmethod
    def remove_outliers_iqr(df: pd.DataFrame, columns: List[str], factor: float = 1.5) -> pd.DataFrame:
        """
        Remove outliers using IQR method
        
        Args:
            df: Input DataFrame
            columns: Columns to check for outliers
            factor: IQR multiplier
            
        Returns:
            DataFrame without outliers
        """
        df = df.copy()
        
        for col in columns:
            if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                outlier_mask = DataCleaner.detect_outliers_iqr(df, col, factor)
                df = df[~outlier_mask]
        
        return df
    
    @staticmethod
    def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize column names (lowercase, underscores)
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with standardized column names
        """
        df = df.copy()
        df.columns = [col.lower().strip().replace(' ', '_') for col in df.columns]
        return df


# Convenience functions
def trim(df: pd.DataFrame, columns: Optional[List[str]] = None) -> pd.DataFrame:
    """Trim whitespace - convenience function"""
    return DataCleaner.trim_whitespace(df, columns)


def fillna(df: pd.DataFrame, strategy: str = 'mean', columns: Optional[List[str]] = None) -> pd.DataFrame:
    """Fill missing values - convenience function"""
    return DataCleaner.handle_missing_data(df, strategy, columns)


def typecast(df: pd.DataFrame, column: str, target_type: str) -> pd.DataFrame:
    """Type cast column - convenience function"""
    return DataCleaner.typecast_column(df, column, target_type)


# Example usage
if __name__ == "__main__":
    # Create sample data
    data = {
        'name': ['  John  ', 'Jane', '  Bob  ', 'Alice', 'John'],
        'age': [25, np.nan, 35, 28, 25],
        'email': ['john@example.com', 'invalid-email', 'bob@test.com', 'alice@test.com', 'john@example.com'],
        'salary': [50000, 60000, 200000, 55000, 50000]  # 200000 is outlier
    }
    df = pd.DataFrame(data)
    
    print("Original Data:")
    print(df)
    print("\n" + "="*50 + "\n")
    
    # Apply cleaning utilities
    df = DataCleaner.trim_whitespace(df)
    print("After trimming whitespace:")
    print(df)
    print("\n" + "="*50 + "\n")
    
    df = DataCleaner.fill_missing_mean(df, ['age'])
    print("After filling missing age with mean:")
    print(df)
    print("\n" + "="*50 + "\n")
    
    df = DataCleaner.remove_duplicates(df, subset=['name', 'email'])
    print("After removing duplicates:")
    print(df)
    print("\n" + "="*50 + "\n")
    
    df = DataCleaner.validate_email(df, 'email', drop_invalid=True)
    print("After validating emails:")
    print(df)
    print("\n" + "="*50 + "\n")
    
    df = DataCleaner.remove_outliers_iqr(df, ['salary'])
    print("After removing outliers:")
    print(df)
