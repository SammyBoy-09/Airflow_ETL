"""
Simplified Phase 2 Tests - Focus on core functionality
"""

import pandas as pd
import sys
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add scripts to path
sys.path.insert(0, str(Path(__file__).parent / 'scripts'))

from utils.db_utils import DatabaseManager
from utils.validation_utils import DataValidator
from utils.transformation_utils import DataTransformer
from utils.duplicate_missing_handler import DuplicateHandler, MissingDataHandler

def test_phase2():
    """Test Phase 2 utilities"""
    
    logger.info("=" * 80)
    logger.info("PHASE 2 TEST: Data Quality & Cleaning Utilities")
    logger.info("=" * 80)
    
    # Load Customers data
    logger.info("\n1️⃣  LOADING DATA...")
    try:
        customers_df = pd.read_csv('data/raw/dataset/Customers.csv')
        logger.info(f"✅ Loaded Customers: {len(customers_df)} rows × {len(customers_df.columns)} columns")
    except Exception as e:
        logger.error(f"❌ Failed to load Customers: {e}")
        return
    
    # Test DataValidator
    logger.info("\n2️⃣  VALIDATING DATA QUALITY...")
    
    # Detect types
    types = DataValidator.detect_data_types(customers_df)
    logger.info("   Data Types Detected:")
    for col in ['Age', 'Birthday', 'Email']:
        if col in types:
            logger.info(f"      {col}: {types[col]}")
    
    # Check nulls per column
    logger.info("\n   Null Values:")
    for col in ['Age', 'Email', 'Name']:
        if col in customers_df.columns:
            null_count = customers_df[col].isnull().sum()
            pct = (null_count / len(customers_df)) * 100
            logger.info(f"      {col}: {null_count} ({pct:.1f}%)")
    
    # Detect duplicates
    logger.info("\n3️⃣  CHECKING FOR DUPLICATES...")
    clean_df, dup_df, count = DuplicateHandler.detect_duplicates(customers_df, ['Email', 'Phone'])
    logger.info(f"   Found {count} duplicate records")
    logger.info(f"   Rows after dedup: {len(clean_df)}")
    
    # Test transformations
    logger.info("\n4️⃣  TESTING TRANSFORMATIONS...")
    
    # Type conversion
    if 'Age' in customers_df.columns:
        age_converted = DataTransformer.safe_typecast(customers_df['Age'].astype(str), 'int', 'coerce')
        logger.info(f"   Age conversion: {customers_df['Age'].dtype} → int")
        logger.info(f"      Original type: {customers_df['Age'].dtype}")
        logger.info(f"      Converted type: {age_converted.dtype}")
        logger.info(f"      Failed: {age_converted.isna().sum()}")
    
    # Date standardization
    if 'Birthday' in customers_df.columns:
        dates_std, failed_idx = DataTransformer.standardize_date_format(
            customers_df['Birthday'].astype(str),
            ['%Y-%m-%d', '%m/%d/%Y', '%d-%m-%Y'],
            '%d-%m-%Y'
        )
        logger.info(f"   Birthday standardization to DD-MM-YYYY:")
        logger.info(f"      Successful: {len(dates_std) - len(failed_idx)}")
        logger.info(f"      Failed: {len(failed_idx)}")
        logger.info(f"      Sample: {dates_std.head(3).tolist()}")
    
    # Missing value imputation
    logger.info("\n5️⃣  MISSING VALUE STRATEGIES...")
    if 'Age' in customers_df.columns:
        original_missing = customers_df['Age'].isnull().sum()
        
        filled_mean, _ = DataTransformer.fill_missing_mean(customers_df['Age'])
        filled_median, _ = DataTransformer.fill_missing_median(customers_df['Age'])
        filled_mode, _ = DataTransformer.fill_missing_mode(customers_df['Age'])
        filled_custom, _ = DataTransformer.fill_missing_custom(customers_df['Age'], 30)
        
        logger.info(f"   Original missing values: {original_missing}")
        logger.info(f"   Mean fill: {filled_mean.isnull().sum()} remaining")
        logger.info(f"   Median fill: {filled_median.isnull().sum()} remaining")
        logger.info(f"   Mode fill: {filled_mode.isnull().sum()} remaining")
        logger.info(f"   Custom (30): {filled_custom.isnull().sum()} remaining")
    
    # Test Database Connection
    logger.info("\n6️⃣  DATABASE CONNECTION...")
    try:
        db = DatabaseManager()
        if db.test_connection():
            logger.info("✅ Connected to PostgreSQL")
            logger.info(f"   Version: {db.get_db_version()}")
            
            # Test rejection logging
            db.log_rejection(
                source_table='customers',
                record_id='test_001',
                error_type='duplicate',
                error_details='Test rejection log from Phase 2 test',
                rejected_data={'email': 'test@example.com', 'phone': '1234567890'}
            )
            logger.info("✅ Logged test rejection to database")
            
            count = db.get_rejection_count('customers')
            logger.info(f"   Total rejections for customers: {count}")
            
            db.close()
        else:
            logger.error("❌ Failed to connect to database")
    except Exception as e:
        logger.error(f"❌ Database error: {e}")
    
    logger.info("\n" + "=" * 80)
    logger.info("✅ PHASE 2 TESTS COMPLETE")
    logger.info("=" * 80)

if __name__ == '__main__':
    test_phase2()
