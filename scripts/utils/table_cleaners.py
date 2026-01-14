############# TEAM 1 - T0008-T0012: Table-Specific Data Cleaning #############

"""
Table-Specific Data Cleaning Utilities
Tasks: T0008-T0012 - Build reusable cleaning utilities with table-specific logic

Cleans:
- Customers: Deduplication, birthday standardization, age typecasting, email validation
- Sales: Date standardization, delivery status, JOIN Products for total_amount
- Products: Standard cleaning
- Stores: Standard cleaning  
- Exchange_Rates: Standard cleaning
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import re
import logging
from typing import Dict, Any, Optional, Tuple
from pathlib import Path

logger = logging.getLogger(__name__)


class CustomersCleaner:
    """
    TEAM 1 - T0008: Customers table cleaning
    - Remove duplicate rows
    - Standardize birthday format
    - Typecast age to int
    - Fill missing age with mean
    - Validate and fix emails
    """
    
    @staticmethod
    def clean(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Clean customers table
        
        Returns:
            (cleaned_df, stats_dict)
        """
        logger.info("▶ TEAM 1 - T0008: Cleaning Customers table")
        
        stats = {
            'input_rows': len(df),
            'duplicates_removed': 0,
            'birthdays_standardized': 0,
            'ages_typecasted': 0,
            'missing_ages_filled': 0,
            'invalid_emails_fixed': 0
        }
        
        df_clean = df.copy()
        
        # 1. Remove duplicates
        initial_rows = len(df_clean)
        df_clean = df_clean.drop_duplicates(subset=['CustomerKey'], keep='first')
        stats['duplicates_removed'] = initial_rows - len(df_clean)
        logger.info(f"   ✅ Removed {stats['duplicates_removed']} duplicate customers")
        
        # 2. Standardize birthday to YYYY-MM-DD
        if 'Birthday' in df_clean.columns:
            df_clean['Birthday'] = pd.to_datetime(df_clean['Birthday'], errors='coerce')
            stats['birthdays_standardized'] = df_clean['Birthday'].notna().sum()
            logger.info(f"   ✅ Standardized {stats['birthdays_standardized']} birthdays")
        
        # 3. Typecast age to int
        if 'Age' in df_clean.columns:
            df_clean['Age'] = pd.to_numeric(df_clean['Age'], errors='coerce')
            missing_ages = df_clean['Age'].isna().sum()
            
            # 4. Fill missing age with mean
            if missing_ages > 0:
                mean_age = df_clean['Age'].mean()
                df_clean['Age'] = df_clean['Age'].fillna(mean_age)
                df_clean['Age'] = df_clean['Age'].astype(int)
                stats['missing_ages_filled'] = missing_ages
                logger.info(f"   ✅ Filled {missing_ages} missing ages with mean: {mean_age:.1f}")
            else:
                df_clean['Age'] = df_clean['Age'].astype(int)
            
            stats['ages_typecasted'] = len(df_clean)
        
        # 5. Validate and fix emails
        if 'Email' in df_clean.columns:
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            
            def validate_email(email):
                if pd.isna(email):
                    return 'no-email@unknown.com'
                if re.match(email_pattern, str(email)):
                    return email
                return 'invalid-email@unknown.com'
            
            invalid_before = (~df_clean['Email'].apply(lambda x: bool(re.match(email_pattern, str(x))) if pd.notna(x) else False)).sum()
            df_clean['Email'] = df_clean['Email'].apply(validate_email)
            stats['invalid_emails_fixed'] = invalid_before
            logger.info(f"   ✅ Fixed {invalid_before} invalid/missing emails")
        
        stats['output_rows'] = len(df_clean)
        logger.info(f"✅ Customers cleaning complete: {stats['input_rows']:,} → {stats['output_rows']:,} rows\n")
        
        return df_clean, stats


class SalesCleaner:
    """
    TEAM 1 - T0009-T0011: Sales table cleaning
    - Standardize dates
    - Add delivery status column
    - JOIN Products to calculate total_amount
    - Fill missing dates with 'NA'
    """
    
    @staticmethod
    def clean(sales_df: pd.DataFrame, products_df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Clean sales table with Product JOIN
        
        Args:
            sales_df: Sales DataFrame
            products_df: Products DataFrame (for price lookup)
        
        Returns:
            (cleaned_df, stats_dict)
        """
        logger.info("▶ TEAM 1 - T0009-T0011: Cleaning Sales table")
        
        stats = {
            'input_rows': len(sales_df),
            'dates_standardized': 0,
            'missing_dates_filled': 0,
            'delivery_status_added': 0,
            'total_amount_calculated': 0
        }
        
        df_clean = sales_df.copy()
        
        # 1. Standardize dates (T0009)
        date_cols = ['Order Date', 'Delivery Date']
        for col in date_cols:
            if col in df_clean.columns:
                df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
                stats['dates_standardized'] += df_clean[col].notna().sum()
        
        logger.info(f"   ✅ Standardized {stats['dates_standardized']} dates")
        
        # 2. Fill missing delivery dates with 'NA' string (keep as object type for this)
        if 'Delivery Date' in df_clean.columns:
            missing_delivery = df_clean['Delivery Date'].isna().sum()
            # Convert to string representation for NA
            df_clean['Delivery Date String'] = df_clean['Delivery Date'].apply(
                lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else 'NA'
            )
            stats['missing_dates_filled'] = missing_delivery
            logger.info(f"   ✅ Filled {missing_delivery} missing delivery dates with 'NA'")
        
        # 3. Add delivery status column (T0010)
        df_clean['Delivery_Status'] = df_clean.apply(
            SalesCleaner._get_delivery_status, axis=1
        )
        stats['delivery_status_added'] = len(df_clean)
        logger.info(f"   ✅ Added delivery status for {len(df_clean):,} orders")
        
        # 4. JOIN Products to calculate Total_Amount_USD (T0011)
        if 'ProductKey' in df_clean.columns and 'Unit Price USD' in products_df.columns:
            df_clean = df_clean.merge(
                products_df[['ProductKey', 'Unit Price USD']],
                on='ProductKey',
                how='left'
            )
            
            df_clean['Total_Amount_USD'] = df_clean['Quantity'] * df_clean['Unit Price USD']
            df_clean['Total_Amount_USD'] = df_clean['Total_Amount_USD'].fillna(0)
            
            stats['total_amount_calculated'] = (df_clean['Total_Amount_USD'] > 0).sum()
            logger.info(f"   ✅ Calculated Total_Amount_USD for {stats['total_amount_calculated']:,} orders")
        
        stats['output_rows'] = len(df_clean)
        logger.info(f"✅ Sales cleaning complete: {stats['input_rows']:,} → {stats['output_rows']:,} rows\n")
        
        return df_clean, stats
    
    @staticmethod
    def _get_delivery_status(row, reference_date=None):
        """
        Determine delivery status based on dates
        
        Logic:
        - If Delivery Date exists: "Delivered"
        - If no Delivery Date:
            - ≤30 days since order: "Delivering Soon"
            - 31-365 days: "To Be Shipped"
            - >365 days: "Lost"
        """
        if reference_date is None:
            reference_date = datetime.now()
        
        # If delivered
        if pd.notna(row['Delivery Date']):
            return 'Delivered'
        
        # If no order date
        if pd.isna(row['Order Date']):
            return 'Unknown'
        
        # Calculate days since order
        days_since_order = (reference_date - row['Order Date']).days
        
        if days_since_order <= 30:
            return 'Delivering Soon'
        elif days_since_order <= 365:
            return 'To Be Shipped'
        else:
            return 'Lost'


class ProductsCleaner:
    """
    TEAM 1 - T0008: Products table cleaning
    - Remove duplicates
    - Standardize formats
    """
    
    @staticmethod
    def clean(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Clean products table"""
        logger.info("▶ TEAM 1 - T0008: Cleaning Products table")
        
        stats = {
            'input_rows': len(df),
            'duplicates_removed': 0
        }
        
        df_clean = df.copy()
        
        # Remove duplicates
        initial_rows = len(df_clean)
        df_clean = df_clean.drop_duplicates(subset=['ProductKey'], keep='first')
        stats['duplicates_removed'] = initial_rows - len(df_clean)
        
        stats['output_rows'] = len(df_clean)
        logger.info(f"✅ Products cleaning complete: {stats['input_rows']:,} → {stats['output_rows']:,} rows\n")
        
        return df_clean, stats


class StoresCleaner:
    """
    TEAM 1 - T0008: Stores table cleaning
    - Remove duplicates
    - Standardize formats
    """
    
    @staticmethod
    def clean(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Clean stores table"""
        logger.info("▶ TEAM 1 - T0008: Cleaning Stores table")
        
        stats = {
            'input_rows': len(df),
            'duplicates_removed': 0
        }
        
        df_clean = df.copy()
        
        # Remove duplicates
        initial_rows = len(df_clean)
        df_clean = df_clean.drop_duplicates(subset=['StoreKey'], keep='first')
        stats['duplicates_removed'] = initial_rows - len(df_clean)
        
        stats['output_rows'] = len(df_clean)
        logger.info(f"✅ Stores cleaning complete: {stats['input_rows']:,} → {stats['output_rows']:,} rows\n")
        
        return df_clean, stats


class ExchangeRatesCleaner:
    """
    TEAM 1 - T0008: Exchange Rates table cleaning
    - Remove duplicates
    - Standardize date formats
    """
    
    @staticmethod
    def clean(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Clean exchange rates table"""
        logger.info("▶ TEAM 1 - T0008: Cleaning Exchange Rates table")
        
        stats = {
            'input_rows': len(df),
            'duplicates_removed': 0,
            'dates_standardized': 0
        }
        
        df_clean = df.copy()
        
        # Standardize date if exists
        if 'Date' in df_clean.columns:
            df_clean['Date'] = pd.to_datetime(df_clean['Date'], errors='coerce')
            stats['dates_standardized'] = df_clean['Date'].notna().sum()
        
        # Remove duplicates (assuming combination of Date + Currency is unique)
        initial_rows = len(df_clean)
        if 'Date' in df_clean.columns and 'Currency' in df_clean.columns:
            df_clean = df_clean.drop_duplicates(subset=['Date', 'Currency'], keep='first')
        stats['duplicates_removed'] = initial_rows - len(df_clean)
        
        stats['output_rows'] = len(df_clean)
        logger.info(f"✅ Exchange Rates cleaning complete: {stats['input_rows']:,} → {stats['output_rows']:,} rows\n")
        
        return df_clean, stats


# ========================================
# TEAM 1 - T0012: Config-Driven Cleaning Orchestrator
# ========================================

class TableCleaningOrchestrator:
    """
    Orchestrate cleaning of all tables
    """
    
    @staticmethod
    def clean_all_tables(data_dir: Path) -> Dict[str, Any]:
        """
        Clean all 5 tables
        
        Args:
            data_dir: Path to data/raw/dataset directory
        
        Returns:
            Dictionary with cleaned DataFrames and stats
        """
        logger.info("\n" + "="*70)
        logger.info("TEAM 1 - T0008-T0012: TABLE CLEANING PIPELINE")
        logger.info("="*70 + "\n")
        
        results = {
            'cleaned_tables': {},
            'stats': {},
            'success': True
        }
        
        try:
            # Load raw data
            customers_raw = pd.read_csv(data_dir / 'Customers.csv')
            sales_raw = pd.read_csv(data_dir / 'Sales.csv')
            products_raw = pd.read_csv(data_dir / 'Products.csv')
            stores_raw = pd.read_csv(data_dir / 'Stores.csv')
            exchange_rates_raw = pd.read_csv(data_dir / 'Exchange_Rates.csv')
            
            # Clean each table
            customers_clean, customers_stats = CustomersCleaner.clean(customers_raw)
            products_clean, products_stats = ProductsCleaner.clean(products_raw)
            sales_clean, sales_stats = SalesCleaner.clean(sales_raw, products_clean)
            stores_clean, stores_stats = StoresCleaner.clean(stores_raw)
            exchange_rates_clean, exchange_rates_stats = ExchangeRatesCleaner.clean(exchange_rates_raw)
            
            # Store results
            results['cleaned_tables'] = {
                'customers': customers_clean,
                'sales': sales_clean,
                'products': products_clean,
                'stores': stores_clean,
                'exchange_rates': exchange_rates_clean
            }
            
            results['stats'] = {
                'customers': customers_stats,
                'sales': sales_stats,
                'products': products_stats,
                'stores': stores_stats,
                'exchange_rates': exchange_rates_stats
            }
            
            logger.info("="*70)
            logger.info("✅ ALL TABLES CLEANED SUCCESSFULLY")
            logger.info("="*70 + "\n")
            
        except Exception as e:
            logger.error(f"❌ Table cleaning failed: {e}")
            results['success'] = False
            results['error'] = str(e)
        
        return results
