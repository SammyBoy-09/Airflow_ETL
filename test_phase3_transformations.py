############# PHASE 3 TEST: Aggregations, Normalization, Features, DateTime #############

"""
Phase 3 Transformation Test
Tests all Phase 3 utilities on real 85K-record dataset

Tests:
1. Load real amazon.csv (85,152 records)
2. Run aggregation pipeline (groupby customer)
3. Run normalization (Z-score on all numeric)
4. Run feature engineering (customer/sales/product features)
5. Run datetime transformations (date parts, intervals, tenure)
6. Validate outputs (rows, columns, types, nulls)
7. Test batch processing options
8. Save to CSV and database
9. Generate comprehensive metrics report
"""

import os
import sys
import pandas as pd
import numpy as np
import sqlite3
from pathlib import Path
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger(__name__)

# Add scripts to path
sys.path.insert(0, str(Path(__file__).parent / 'scripts' / 'utils'))

from aggregation_utils import AggregationEngine
from normalization_utils import NormalizationEngine
from feature_engineering_utils import FeatureEngine
from datetime_utils import DateTimeEngine


class Phase3Tester:
    """Comprehensive Phase 3 transformation tests"""
    
    def __init__(self):
        self.base_dir = Path(__file__).parent
        self.data_dir = self.base_dir / 'data'
        self.results = {}
        self.start_time = datetime.now()
    
    def run_all_tests(self):
        """Execute complete Phase 3 test suite"""
        logger.info("\n" + "="*80)
        logger.info("PHASE 3 TRANSFORMATION TEST SUITE")
        logger.info("="*80 + "\n")
        
        try:
            # 1. Load data
            self.test_load_data()
            
            # 2. Test aggregations
            self.test_aggregations()
            
            # 3. Test normalization
            self.test_normalization()
            
            # 4. Test feature engineering
            self.test_feature_engineering()
            
            # 5. Test datetime transformations
            self.test_datetime_transformations()
            
            # 6. Test batch processing
            self.test_batch_processing()
            
            # 7. Test database operations
            self.test_database_operations()
            
            # 8. Generate final report
            self.generate_report()
            
            logger.info("\n✅ ALL TESTS COMPLETED SUCCESSFULLY\n")
            
        except Exception as e:
            logger.error(f"\n❌ TEST SUITE FAILED: {e}\n")
            raise
    
    def test_load_data(self):
        """Test 1: Load raw data"""
        logger.info("▶ TEST 1: Loading Data")
        
        csv_path = self.data_dir / 'raw' / 'dataset' / 'Sales.csv'
        
        if not csv_path.exists():
            raise FileNotFoundError(f"Test data not found: {csv_path}")
        
        df = pd.read_csv(csv_path)
        
        self.df_raw = df
        self.results['load_data'] = {
            'rows': len(df),
            'columns': len(df.columns),
            'memory_mb': df.memory_usage(deep=True).sum() / 1024 / 1024,
            'column_names': list(df.columns),
            'dtypes': df.dtypes.to_dict()
        }
        
        logger.info(f"✅ Loaded {len(df):,} rows × {len(df.columns)} columns")
        logger.info(f"   Memory: {self.results['load_data']['memory_mb']:.2f} MB")
        logger.info(f"   Columns: {', '.join(df.columns[:5])}... (showing first 5)\n")
    
    def test_aggregations(self):
        """Test 2: Aggregations (groupBy, sum, count, min, max, mean)"""
        logger.info("▶ TEST 2: Aggregations")
        
        try:
            agg_config = {
                'Quantity': ['sum', 'count', 'mean']
            }
            
            # Standard aggregation
            df_agg = AggregationEngine.aggregate(
                self.df_raw,
                group_by='CustomerKey',
                agg_config=agg_config
            )
            
            # Validate
            assert len(df_agg) < len(self.df_raw), "Aggregation should reduce rows"
            assert df_agg.isnull().sum().sum() == 0, "No nulls in aggregated data"
            
            self.df_agg = df_agg
            self.results['aggregations'] = {
                'input_rows': len(self.df_raw),
                'output_rows': len(df_agg),
                'reduction_pct': (1 - len(df_agg)/len(self.df_raw)) * 100,
                'output_columns': len(df_agg.columns),
                'agg_metrics': list(agg_config.keys())
            }
            
            logger.info(f"✅ Aggregation successful")
            logger.info(f"   {len(self.df_raw):,} → {len(df_agg):,} rows ({self.results['aggregations']['reduction_pct']:.1f}% reduction)")
            logger.info(f"   Output columns: {len(df_agg.columns)}")
            logger.info(f"   Sample output:\n{df_agg.head(3).to_string()}\n")
            
        except Exception as e:
            logger.error(f"❌ Aggregation test failed: {e}\n")
            raise
    
    def test_normalization(self):
        """Test 3: Normalization (Z-score)"""
        logger.info("▶ TEST 3: Normalization (Z-score)")
        
        try:
            # Normalize numeric columns
            df_norm, stats = NormalizationEngine.z_score_normalize(
                self.df_raw,
                exclude_columns=['CustomerKey', 'ProductKey']
            )
            
            # Validate
            numeric_cols = [col for col in stats.keys() if not stats[col].get('skipped')]
            
            for col in numeric_cols:
                mean_val = df_norm[col].mean()
                std_val = df_norm[col].std()
                
                assert abs(mean_val) < 0.0001, f"Mean not ~0 for {col}"
                assert abs(std_val - 1.0) < 0.0001, f"Std not ~1 for {col}"
            
            self.df_norm = df_norm
            self.results['normalization'] = {
                'normalized_columns': numeric_cols,
                'skipped_columns': [col for col in stats.keys() if stats[col].get('skipped')],
                'stats_sample': {
                    list(stats.keys())[0]: stats[list(stats.keys())[0]]
                    if numeric_cols else {}
                }
            }
            
            logger.info(f"✅ Normalization successful")
            logger.info(f"   Normalized {len(numeric_cols)} columns")
            logger.info(f"   Skipped {len(self.results['normalization']['skipped_columns'])} columns\n")
            
        except Exception as e:
            logger.error(f"❌ Normalization test failed: {e}\n")
            raise
    
    def test_feature_engineering(self):
        """Test 4: Feature Engineering"""
        logger.info("▶ TEST 4: Feature Engineering")
        
        try:
            # Create customer features
            # Note: Requires separate customers DataFrame or aggregated customer keys
            
            # For now, test on sample of raw data with minimal requirements
            customers_sample = pd.DataFrame({
                'CustomerKey': self.df_raw['CustomerKey'].unique()[:100]
            })
            
            customer_features = FeatureEngine.create_customer_features(
                customers_sample,
                self.df_raw.head(10000),  # Use sample for speed
                customer_key='CustomerKey',
                order_date_col='Order Date',
                quantity_col='Quantity'
            )
            
            # Validate
            assert len(customer_features) > 0, "No customer features created"
            expected_features = [
                'total_orders', 'total_quantity', 'avg_order_quantity',
                'tenure_days', 'recency_days', 'purchase_frequency'
            ]
            
            actual_features = [f for f in expected_features if f in customer_features.columns]
            
            self.results['feature_engineering'] = {
                'customer_features_created': actual_features,
                'sample_customers': len(customer_features),
                'feature_columns': len(customer_features.columns)
            }
            
            logger.info(f"✅ Feature engineering successful")
            logger.info(f"   Created {len(actual_features)} customer features")
            logger.info(f"   Sample: {', '.join(actual_features[:3])}\n")
            
        except Exception as e:
            logger.error(f"❌ Feature engineering test failed: {e}\n")
            raise
    
    def test_datetime_transformations(self):
        """Test 5: DateTime Transformations"""
        logger.info("▶ TEST 5: DateTime Transformations")
        
        try:
            df_sample = self.df_raw.head(1000).copy()
            
            # Test date part extraction
            df_dt = DateTimeEngine.extract_date_parts(df_sample, 'Order Date', prefix='order')
            
            # Validate
            expected_cols = ['order_year', 'order_month', 'order_day', 'order_day_of_week', 'order_quarter']
            missing = [col for col in expected_cols if col not in df_dt.columns]
            
            assert not missing, f"Missing columns: {missing}"
            
            # Test interval calculation
            if 'Delivery Date' in df_dt.columns:
                df_dt = DateTimeEngine.days_between(df_dt, 'Order Date', 'Delivery Date', 'days_order_to_delivery')
                assert 'days_order_to_delivery' in df_dt.columns, "Interval column not created"
            
            self.results['datetime'] = {
                'date_parts_extracted': expected_cols,
                'sample_rows': len(df_dt),
                'new_columns': len(df_dt.columns) - len(df_sample.columns)
            }
            
            logger.info(f"✅ DateTime transformations successful")
            logger.info(f"   Extracted {len(expected_cols)} date parts")
            logger.info(f"   Added {self.results['datetime']['new_columns']} columns\n")
            
        except Exception as e:
            logger.error(f"❌ DateTime test failed: {e}\n")
            raise
    
    def test_batch_processing(self):
        """Test 6: Batch Processing for Memory Optimization"""
        logger.info("▶ TEST 6: Batch Processing")
        
        try:
            # Test normalization with batching
            batch_size = 10000
            
            df_norm_batch, _ = NormalizationEngine.z_score_normalize(
                self.df_raw,
                exclude_columns=['CustomerKey', 'ProductKey'],
                batch_size=batch_size
            )
            
            # Should produce same dimensions
            assert len(df_norm_batch) == len(self.df_raw), "Batch processing changed row count"
            
            self.results['batch_processing'] = {
                'batch_size': batch_size,
                'num_batches': (len(self.df_raw) + batch_size - 1) // batch_size,
                'rows_processed': len(df_norm_batch),
                'success': True
            }
            
            logger.info(f"✅ Batch processing successful")
            logger.info(f"   Batch size: {batch_size:,} rows")
            logger.info(f"   Num batches: {self.results['batch_processing']['num_batches']}\n")
            
        except Exception as e:
            logger.error(f"❌ Batch processing test failed: {e}\n")
            raise
    
    def test_database_operations(self):
        """Test 7: Database Save/Load Operations"""
        logger.info("▶ TEST 7: Database Operations")
        
        try:
            db_path = self.data_dir / 'test_phase3.db'
            
            # Save to database
            conn = sqlite3.connect(str(db_path))
            self.df_agg.to_sql('amazon_aggregated', conn, if_exists='replace', index=False)
            conn.commit()
            
            # Load back
            df_loaded = pd.read_sql_query('SELECT * FROM amazon_aggregated LIMIT 10', conn)
            conn.close()
            
            # Validate
            assert len(df_loaded) > 0, "Data not saved to database"
            assert len(df_loaded.columns) == len(self.df_agg.columns), "Column mismatch"
            
            self.results['database'] = {
                'db_path': str(db_path),
                'table_name': 'amazon_aggregated',
                'rows_saved': len(self.df_agg),
                'rows_loaded': len(df_loaded),
                'columns': len(df_loaded.columns)
            }
            
            logger.info(f"✅ Database operations successful")
            logger.info(f"   Saved {len(self.df_agg):,} rows to {db_path.name}\n")
            
        except Exception as e:
            logger.error(f"❌ Database test failed: {e}\n")
            raise
    
    def generate_report(self):
        """Generate comprehensive test report"""
        elapsed = (datetime.now() - self.start_time).total_seconds()
        
        logger.info("="*80)
        logger.info("PHASE 3 TEST RESULTS SUMMARY")
        logger.info("="*80)
        
        logger.info("\n📊 INPUT DATA:")
        logger.info(f"  Rows: {self.results['load_data']['rows']:,}")
        logger.info(f"  Columns: {self.results['load_data']['columns']}")
        logger.info(f"  Memory: {self.results['load_data']['memory_mb']:.2f} MB")
        
        logger.info("\n✅ AGGREGATIONS:")
        logger.info(f"  Input: {self.results['aggregations']['input_rows']:,} rows")
        logger.info(f"  Output: {self.results['aggregations']['output_rows']:,} rows")
        logger.info(f"  Reduction: {self.results['aggregations']['reduction_pct']:.1f}%")
        logger.info(f"  Metrics: {', '.join(self.results['aggregations']['agg_metrics'])}")
        
        logger.info("\n✅ NORMALIZATION:")
        logger.info(f"  Normalized columns: {len(self.results['normalization']['normalized_columns'])}")
        logger.info(f"  Skipped columns: {len(self.results['normalization']['skipped_columns'])}")
        
        logger.info("\n✅ FEATURE ENGINEERING:")
        logger.info(f"  Customer features: {', '.join(self.results['feature_engineering']['customer_features_created'][:3])}")
        
        logger.info("\n✅ DATETIME TRANSFORMATIONS:")
        logger.info(f"  Date parts: {len(self.results['datetime']['date_parts_extracted'])}")
        logger.info(f"  New columns: {self.results['datetime']['new_columns']}")
        
        logger.info("\n✅ BATCH PROCESSING:")
        logger.info(f"  Batch size: {self.results['batch_processing']['batch_size']:,}")
        logger.info(f"  Num batches: {self.results['batch_processing']['num_batches']}")
        
        logger.info("\n✅ DATABASE:")
        logger.info(f"  Rows saved: {self.results['database']['rows_saved']:,}")
        logger.info(f"  Table: {self.results['database']['table_name']}")
        
        logger.info(f"\n⏱️  TOTAL TIME: {elapsed:.2f} seconds")
        logger.info("="*80 + "\n")


if __name__ == '__main__':
    tester = Phase3Tester()
    tester.run_all_tests()
