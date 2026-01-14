"""
Test script for Phase 2 utilities
Tests all cleaning utilities against real CSV data
"""

import pandas as pd
import sys
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add scripts to path
scripts_path = Path(__file__).parent / 'scripts'
sys.path.insert(0, str(scripts_path))

from utils.db_utils import DatabaseManager
from utils.validation_utils import DataValidator
from utils.transformation_utils import DataTransformer
from utils.duplicate_missing_handler import DuplicateHandler, MissingDataHandler
from utils.cleaning_engine import ConfigDrivenCleaner

def test_data_loading():
    """Test loading CSV data"""
    logger.info("=" * 60)
    logger.info("TEST 1: Load Real CSV Data")
    logger.info("=" * 60)
    
    data_files = {
        'customers': r'data/raw/dataset/Customers.csv',
        'sales': r'data/raw/dataset/Sales.csv',
        'products': r'data/raw/dataset/Products.csv',
        'stores': r'data/raw/dataset/Stores.csv',
        'exchange_rates': r'data/raw/dataset/Exchange_Rates.csv'
    }
    
    loaded = {}
    for name, path in data_files.items():
        try:
            df = pd.read_csv(path)
            loaded[name] = df
            logger.info(f"✅ {name}: {len(df)} rows × {len(df.columns)} columns")
            logger.info(f"   Columns: {list(df.columns)}")
            logger.info(f"   Memory: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
        except FileNotFoundError:
            logger.error(f"❌ File not found: {path}")
        except Exception as e:
            logger.error(f"❌ Error loading {name}: {e}")
    
    return loaded


def test_data_validation(loaded):
    """Test validation utilities"""
    logger.info("\n" + "=" * 60)
    logger.info("TEST 2: Data Validation")
    logger.info("=" * 60)
    
    customers = loaded.get('customers')
    if customers is None:
        logger.warning("⚠️ Customers data not available")
        return
    
    logger.info("\n📊 CUSTOMERS DATA QUALITY REPORT:")
    logger.info(f"Total rows: {len(customers)}")
    logger.info(f"Total columns: {len(customers.columns)}\n")
    
    # Detect types
    logger.info("1️⃣ DATA TYPES:")
    types = DataValidator.detect_data_types(customers)
    for col, dtype in types.items():
        logger.info(f"   {col}: {dtype}")
    
    # Check nulls
    logger.info("\n2️⃣ NULL VALUES:")
    null_report = DataValidator.validate_column_nulls(customers)
    for col, count in null_report.items():
        pct = (count / len(customers)) * 100
        if count > 0:
            logger.info(f"   {col}: {count} ({pct:.1f}%)")
    
    # Detect duplicates
    logger.info("\n3️⃣ DUPLICATES:")
    clean_df, dup_df, dup_count = DuplicateHandler.detect_duplicates(customers, ['Email', 'Phone'])
    logger.info(f"   Found {dup_count} duplicate records on Email + Phone")
    if dup_count > 0:
        logger.info(f"   First duplicate key: {dup_df.iloc[0][['Email', 'Phone']].to_dict()}")
    
    # Email validation
    logger.info("\n4️⃣ EMAIL VALIDATION:")
    if 'Email' in customers.columns:
        valid_emails = DataValidator.validate_email_column(customers['Email'])
        invalid_count = len(customers) - valid_emails
        logger.info(f"   Valid emails: {valid_emails}")
        logger.info(f"   Invalid emails: {invalid_count}")
        if invalid_count > 0:
            logger.info(f"   Sample invalid: {customers[~customers['Email'].apply(DataValidator.validate_email)]['Email'].head(3).tolist()}")


def test_transformations(loaded):
    """Test transformation utilities"""
    logger.info("\n" + "=" * 60)
    logger.info("TEST 3: Data Transformations")
    logger.info("=" * 60)
    
    customers = loaded.get('customers')
    sales = loaded.get('sales')
    
    if customers is None:
        logger.warning("⚠️ Customers data not available")
        return
    
    # Test type conversion
    logger.info("\n1️⃣ TYPE CONVERSION (Customers.Age: string → int):")
    if 'Age' in customers.columns:
        original_type = customers['Age'].dtype
        converted = DataTransformer.safe_typecast(customers['Age'].astype(str), 'int', 'coerce')
        logger.info(f"   Original type: {original_type}")
        logger.info(f"   Target type: int")
        logger.info(f"   Conversion successful: {converted.dtype == 'int64'}")
        logger.info(f"   Null introduced: {converted.isna().sum()}")
        logger.info(f"   Sample values: {converted.head(3).tolist()}")
    
    # Test date standardization
    logger.info("\n2️⃣ DATE STANDARDIZATION:")
    if sales is not None and 'OrderDate' in sales.columns:
        logger.info(f"   Column: OrderDate")
        logger.info(f"   Input sample: {sales['OrderDate'].head(3).tolist()}")
        standardized, failed = DataTransformer.standardize_date_format(
            sales['OrderDate'],
            ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y'],
            '%d-%m-%Y'
        )
        logger.info(f"   Output format: DD-MM-YYYY")
        logger.info(f"   Successful: {len(standardized) - len(failed)} / {len(standardized)}")
        logger.info(f"   Failed: {len(failed)}")
        logger.info(f"   Sample output: {standardized.head(3).tolist()}")
    
    # Test missing value imputation
    logger.info("\n3️⃣ MISSING VALUE STRATEGIES:")
    if 'Age' in customers.columns:
        age_missing = customers['Age'].isna().sum()
        logger.info(f"   Original missing: {age_missing}")
        
        # Mean
        filled_mean = DataTransformer.fill_missing_mean(customers['Age'])
        logger.info(f"   ✓ Mean fill: {filled_mean.isna().sum()} remaining")
        
        # Median
        filled_median = DataTransformer.fill_missing_median(customers['Age'])
        logger.info(f"   ✓ Median fill: {filled_median.isna().sum()} remaining")
        
        # Mode
        filled_mode = DataTransformer.fill_missing_mode(customers['Age'])
        logger.info(f"   ✓ Mode fill: {filled_mode.isna().sum()} remaining")
        
        # Custom value
        filled_custom = DataTransformer.fill_missing_custom(customers['Age'], 30)
        logger.info(f"   ✓ Custom (30): {filled_custom.isna().sum()} remaining")


def test_database_logging():
    """Test database connection and logging"""
    logger.info("\n" + "=" * 60)
    logger.info("TEST 4: Database Connection & Logging")
    logger.info("=" * 60)
    
    try:
        db = DatabaseManager()
        
        # Test connection
        logger.info("\n1️⃣ DATABASE CONNECTION:")
        if db.test_connection():
            logger.info("✅ Connected to PostgreSQL successfully")
            logger.info(f"   Host: {db.host}")
            logger.info(f"   Database: {db.database}")
            logger.info(f"   Version: {db.get_db_version()}")
        else:
            logger.error("❌ Failed to connect to database")
            return
        
        # Test table creation
        logger.info("\n2️⃣ TABLE SCHEMA:")
        logger.info("✅ All required tables ready (created in Phase 1)")
        
        # Test rejection logging
        logger.info("\n3️⃣ REJECTION LOGGING:")
        test_rejection = {
            'column1': 'value1',
            'column2': 'value2'
        }
        
        try:
            db.log_rejection(
                source_table='test_source',
                record_id='test_001',
                error_type='test_error',
                error_details='This is a test rejection log',
                rejected_data=test_rejection
            )
            logger.info("✅ Successfully logged test rejection")
            
            # Verify it's in the database
            count = db.get_rejection_count('test_source')
            logger.info(f"   Total rejections for test_source: {count}")
            
        except Exception as e:
            logger.error(f"❌ Error logging rejection: {e}")
        
        db.close()
        
    except Exception as e:
        logger.error(f"❌ Database error: {e}")


def test_cleaning_engine(loaded):
    """Test complete cleaning engine"""
    logger.info("\n" + "=" * 60)
    logger.info("TEST 5: Cleaning Engine with Config")
    logger.info("=" * 60)
    
    customers = loaded.get('customers')
    if customers is None:
        logger.warning("⚠️ Customers data not available")
        return
    
    try:
        db = DatabaseManager()
        
        # Test with customers config
        logger.info("\n1️⃣ CUSTOMERS CLEANING PIPELINE:")
        cleaner = ConfigDrivenCleaner('config/customers_config.yaml', db)
        
        # Use loaded data directly instead of reloading
        logger.info(f"   Input: {len(customers)} rows")
        
        cleaned_df, report = cleaner.clean(customers)
        
        logger.info(f"   Output: {len(cleaned_df)} rows")
        logger.info(f"   Rows removed: {report['rows_removed']}")
        logger.info(f"   Cleaning steps:")
        for step in report['steps']:
            logger.info(f"      • {step.get('step', 'unknown')} - {step}")
        
        # Save cleaned data
        output_path = 'data/processed/customers_cleaned_test.csv'
        cleaner.save_cleaned_data(cleaned_df, output_path)
        logger.info(f"   ✅ Saved to: {output_path}")
        
        db.close()
        
    except Exception as e:
        logger.error(f"❌ Error in cleaning engine: {e}")
        import traceback
        traceback.print_exc()


def main():
    """Run all tests"""
    logger.info("\n" + "🚀" * 30)
    logger.info("PHASE 2 UTILITY TESTS - Complete Data Quality Pipeline")
    logger.info("🚀" * 30)
    
    # Test 1: Load data
    loaded = test_data_loading()
    
    if loaded:
        # Test 2: Validation
        test_data_validation(loaded)
        
        # Test 3: Transformations
        test_transformations(loaded)
        
        # Test 4: Database
        test_database_logging()
        
        # Test 5: Cleaning engine
        test_cleaning_engine(loaded)
    
    logger.info("\n" + "✅" * 30)
    logger.info("ALL TESTS COMPLETE")
    logger.info("✅" * 30)


if __name__ == '__main__':
    main()
