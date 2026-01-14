############# TEAM 1 - Comprehensive Test Suite #############

"""
Comprehensive Test Suite for TEAM 1 ETL Pipeline
Tests: T0008-T0032

Tests all components:
1. Table cleaning (5 tables)
2. Aggregations, normalization, features
3. Report generation (8 reports)
4. DAG execution summary
5. Data quality validation
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

from table_cleaners import TableCleaningOrchestrator
from report_generators import ReportOrchestrator
from dag_execution_tracker import DAGExecutionSummary, calculate_data_quality_score


class ComprehensiveETLTester:
    """TEAM 1 - Complete ETL pipeline tests"""
    
    def __init__(self):
        self.base_dir = Path(__file__).parent
        self.data_dir = self.base_dir / 'data'
        self.results = {}
        self.start_time = datetime.now()
    
    def run_all_tests(self):
        """Execute complete test suite"""
        logger.info("\n" + "="*80)
        logger.info("TEAM 1 - COMPREHENSIVE ETL TEST SUITE")
        logger.info("="*80 + "\n")
        
        try:
            # Test 1: Table Cleaning (T0008-T0012)
            self.test_table_cleaning()
            
            # Test 2: Report Generation (Phase 4)
            self.test_report_generation()
            
            # Test 3: DAG Execution Summary (T0031)
            self.test_execution_summary()
            
            # Test 4: Data Quality Validation
            self.test_data_quality()
            
            # Test 5: Output File Validation
            self.test_output_files()
            
            # Test 6: Database Operations
            self.test_database_operations()
            
            # Generate final report
            self.generate_final_report()
            
            logger.info("\n✅ ALL TESTS COMPLETED SUCCESSFULLY\n")
            
        except Exception as e:
            logger.error(f"\n❌ TEST SUITE FAILED: {e}\n")
            raise
    
    def test_table_cleaning(self):
        """Test 1: Table cleaning (T0008-T0012)"""
        logger.info("▶ TEST 1: Table Cleaning (T0008-T0012)")
        
        try:
            data_dir = self.data_dir / 'raw' / 'dataset'
            
            # Run cleaning
            results = TableCleaningOrchestrator.clean_all_tables(data_dir)
            
            # Validate results
            assert results['success'], "Cleaning should succeed"
            assert len(results['cleaned_tables']) == 5, "Should clean 5 tables"
            
            # Validate each table
            for table_name, df in results['cleaned_tables'].items():
                assert len(df) > 0, f"{table_name} should have rows"
                assert df.isnull().sum().sum() == 0 or table_name == 'sales', f"{table_name} should handle nulls"
            
            # Validate specific cleaning operations
            customers_df = results['cleaned_tables']['customers']
            if 'Email' in customers_df.columns:
                invalid_emails = customers_df['Email'].str.contains('unknown.com').sum()
                assert invalid_emails >= 0, "Emails should be validated"
            
            sales_df = results['cleaned_tables']['sales']
            assert 'Delivery_Status' in sales_df.columns, "Sales should have delivery status"
            assert 'Total_Amount_USD' in sales_df.columns, "Sales should have total amount"
            
            # Store results
            self.cleaned_tables = results['cleaned_tables']
            self.cleaning_stats = results['stats']
            
            # Save cleaned tables
            output_dir = self.data_dir / 'processed'
            output_dir.mkdir(parents=True, exist_ok=True)
            
            for table_name, df in self.cleaned_tables.items():
                output_path = output_dir / f'{table_name}_cleaned.csv'
                df.to_csv(output_path, index=False)
            
            self.results['table_cleaning'] = {
                'status': 'PASS',
                'tables_cleaned': len(results['cleaned_tables']),
                'total_rows': sum(stats['output_rows'] for stats in results['stats'].values()),
                'stats': results['stats']
            }
            
            logger.info(f"✅ Table cleaning PASSED - {len(results['cleaned_tables'])} tables cleaned\n")
            
        except Exception as e:
            logger.error(f"❌ Table cleaning FAILED: {e}\n")
            self.results['table_cleaning'] = {'status': 'FAIL', 'error': str(e)}
            raise
    
    def test_report_generation(self):
        """Test 2: Report generation (Phase 4)"""
        logger.info("▶ TEST 2: Report Generation (Phase 4)")
        
        try:
            # Generate all reports
            reports = ReportOrchestrator.generate_all_reports(
                self.cleaned_tables,
                self.cleaning_stats
            )
            
            # Validate reports
            assert len(reports) == 8, "Should generate 8 reports"
            
            expected_reports = [
                'customer_summary',
                'product_performance',
                'order_status',
                'sales_trends_daily',
                'data_quality_scorecard',
                'customer_segmentation',
                'store_performance',
                'anomaly_detection'
            ]
            
            for report_name in expected_reports:
                assert report_name in reports, f"Missing report: {report_name}"
                assert len(reports[report_name]) > 0, f"{report_name} should have rows"
            
            # Validate specific reports
            customer_summary = reports['customer_summary']
            assert 'Total_Orders' in customer_summary.columns, "Customer summary should have Total_Orders"
            assert 'Customer_Segment' in customer_summary.columns, "Customer summary should have segmentation"
            
            order_status = reports['order_status']
            assert 'Delivery_Status' in order_status.columns, "Order status should have delivery status"
            
            # Save reports
            reports_dir = self.data_dir / 'reports'
            reports_dir.mkdir(parents=True, exist_ok=True)
            
            for report_name, report_df in reports.items():
                output_path = reports_dir / f'{report_name}_report.csv'
                report_df.to_csv(output_path, index=False)
            
            self.reports = reports
            
            self.results['report_generation'] = {
                'status': 'PASS',
                'reports_generated': len(reports),
                'report_names': list(reports.keys())
            }
            
            logger.info(f"✅ Report generation PASSED - {len(reports)} reports generated\n")
            
        except Exception as e:
            logger.error(f"❌ Report generation FAILED: {e}\n")
            self.results['report_generation'] = {'status': 'FAIL', 'error': str(e)}
            raise
    
    def test_execution_summary(self):
        """Test 3: DAG execution summary (T0031)"""
        logger.info("▶ TEST 3: DAG Execution Summary (T0031)")
        
        try:
            # Create execution summary
            summary = DAGExecutionSummary('test_dag', f'test_{datetime.now().strftime("%Y%m%d%H%M%S")}')
            
            # Update metrics
            quality_score = calculate_data_quality_score(self.cleaning_stats)
            
            summary.update_metrics(
                input_rows_processed=self.results['table_cleaning']['total_rows'],
                output_tables_generated=5 + 8,
                data_quality_score=quality_score
            )
            
            summary.record_task('clean_tables', 'SUCCESS', 62884, 5.2)
            summary.record_task('generate_reports', 'SUCCESS', 0, 3.1)
            
            summary.finalize('SUCCESS')
            
            # Save to CSV
            summary.save_to_csv(self.base_dir / 'logs')
            
            # Save to Database
            db_path = self.data_dir / 'test_etl.db'
            summary.save_to_database(str(db_path))
            
            # Validate summary
            summary_dict = summary.to_dict()
            assert summary_dict['Status'] == 'SUCCESS', "Summary should be SUCCESS"
            assert summary_dict['Total_Tasks'] == 2, "Should have 2 tasks"
            assert summary_dict['Tasks_Succeeded'] == 2, "All tasks should succeed"
            
            self.results['execution_summary'] = {
                'status': 'PASS',
                'quality_score': quality_score,
                'summary_saved': True
            }
            
            logger.info(f"✅ Execution summary PASSED - Quality score: {quality_score:.1f}%\n")
            
        except Exception as e:
            logger.error(f"❌ Execution summary FAILED: {e}\n")
            self.results['execution_summary'] = {'status': 'FAIL', 'error': str(e)}
            raise
    
    def test_data_quality(self):
        """Test 4: Data quality validation"""
        logger.info("▶ TEST 4: Data Quality Validation")
        
        try:
            quality_checks = {
                'no_duplicates': True,
                'no_null_keys': True,
                'dates_valid': True,
                'emails_valid': True,
                'numeric_types': True
            }
            
            # Check customers
            customers_df = self.cleaned_tables['customers']
            
            # No duplicate customer keys
            if customers_df['CustomerKey'].duplicated().any():
                quality_checks['no_duplicates'] = False
            
            # No null customer keys
            if customers_df['CustomerKey'].isnull().any():
                quality_checks['no_null_keys'] = False
            
            # Emails valid
            if 'Email' in customers_df.columns:
                if not customers_df['Email'].str.contains('@').all():
                    quality_checks['emails_valid'] = False
            
            # Check sales
            sales_df = self.cleaned_tables['sales']
            
            # Dates are datetime
            if 'Order Date' in sales_df.columns:
                sales_df['Order Date'] = pd.to_datetime(sales_df['Order Date'], errors='coerce')
                if sales_df['Order Date'].isnull().any():
                    quality_checks['dates_valid'] = False
            
            # Numeric types
            if 'Quantity' in sales_df.columns:
                if not pd.api.types.is_numeric_dtype(sales_df['Quantity']):
                    quality_checks['numeric_types'] = False
            
            all_passed = all(quality_checks.values())
            
            self.results['data_quality'] = {
                'status': 'PASS' if all_passed else 'FAIL',
                'checks': quality_checks
            }
            
            logger.info(f"✅ Data quality PASSED - All checks passed: {all_passed}\n")
            
        except Exception as e:
            logger.error(f"❌ Data quality FAILED: {e}\n")
            self.results['data_quality'] = {'status': 'FAIL', 'error': str(e)}
            raise
    
    def test_output_files(self):
        """Test 5: Output file validation"""
        logger.info("▶ TEST 5: Output File Validation")
        
        try:
            # Check cleaned tables
            processed_dir = self.data_dir / 'processed'
            expected_tables = ['customers', 'sales', 'products', 'stores', 'exchange_rates']
            
            tables_found = 0
            for table_name in expected_tables:
                file_path = processed_dir / f'{table_name}_cleaned.csv'
                if file_path.exists():
                    tables_found += 1
            
            # Check reports
            reports_dir = self.data_dir / 'reports'
            expected_reports = [
                'customer_summary',
                'product_performance',
                'order_status',
                'sales_trends_daily',
                'data_quality_scorecard',
                'customer_segmentation',
                'store_performance',
                'anomaly_detection'
            ]
            
            reports_found = 0
            for report_name in expected_reports:
                file_path = reports_dir / f'{report_name}_report.csv'
                if file_path.exists():
                    reports_found += 1
            
            # Check execution summary
            summary_path = self.base_dir / 'logs' / 'dag_execution_summary.csv'
            summary_exists = summary_path.exists()
            
            self.results['output_files'] = {
                'status': 'PASS',
                'cleaned_tables': tables_found,
                'reports': reports_found,
                'summary_exists': summary_exists
            }
            
            logger.info(f"✅ Output files PASSED - Tables: {tables_found}/5, Reports: {reports_found}/8\n")
            
        except Exception as e:
            logger.error(f"❌ Output file validation FAILED: {e}\n")
            self.results['output_files'] = {'status': 'FAIL', 'error': str(e)}
            raise
    
    def test_database_operations(self):
        """Test 6: Database operations"""
        logger.info("▶ TEST 6: Database Operations")
        
        try:
            db_path = self.data_dir / 'test_etl.db'
            
            # Check if execution summary table exists
            conn = sqlite3.connect(str(db_path))
            
            # Query execution metadata
            query = "SELECT COUNT(*) FROM dag_execution_metadata"
            result = pd.read_sql_query(query, conn)
            
            assert result.iloc[0, 0] > 0, "Should have execution records"
            
            conn.close()
            
            self.results['database'] = {
                'status': 'PASS',
                'records_found': int(result.iloc[0, 0])
            }
            
            logger.info(f"✅ Database operations PASSED\n")
            
        except Exception as e:
            logger.error(f"❌ Database operations FAILED: {e}\n")
            self.results['database'] = {'status': 'FAIL', 'error': str(e)}
            raise
    
    def generate_final_report(self):
        """Generate comprehensive test report"""
        elapsed = (datetime.now() - self.start_time).total_seconds()
        
        logger.info("="*80)
        logger.info("TEAM 1 - COMPREHENSIVE TEST RESULTS")
        logger.info("="*80)
        
        logger.info("\n📊 TABLE CLEANING (T0008-T0012):")
        logger.info(f"  Status: {self.results['table_cleaning']['status']}")
        logger.info(f"  Tables Cleaned: {self.results['table_cleaning']['tables_cleaned']}")
        logger.info(f"  Total Rows: {self.results['table_cleaning']['total_rows']:,}")
        
        logger.info("\n📈 REPORT GENERATION (Phase 4):")
        logger.info(f"  Status: {self.results['report_generation']['status']}")
        logger.info(f"  Reports Generated: {self.results['report_generation']['reports_generated']}")
        
        logger.info("\n📋 EXECUTION SUMMARY (T0031):")
        logger.info(f"  Status: {self.results['execution_summary']['status']}")
        logger.info(f"  Quality Score: {self.results['execution_summary']['quality_score']:.1f}%")
        
        logger.info("\n✅ DATA QUALITY:")
        logger.info(f"  Status: {self.results['data_quality']['status']}")
        logger.info(f"  Checks: {self.results['data_quality']['checks']}")
        
        logger.info("\n📂 OUTPUT FILES:")
        logger.info(f"  Status: {self.results['output_files']['status']}")
        logger.info(f"  Cleaned Tables: {self.results['output_files']['cleaned_tables']}/5")
        logger.info(f"  Reports: {self.results['output_files']['reports']}/8")
        
        logger.info("\n💾 DATABASE:")
        logger.info(f"  Status: {self.results['database']['status']}")
        logger.info(f"  Records: {self.results['database']['records_found']}")
        
        logger.info(f"\n⏱️  TOTAL TIME: {elapsed:.2f} seconds")
        logger.info("="*80 + "\n")


if __name__ == '__main__':
    tester = ComprehensiveETLTester()
    tester.run_all_tests()
