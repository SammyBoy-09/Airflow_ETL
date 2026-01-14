# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - ETL PIPELINE: EXTRACT PHASE
# Tasks: T0002, T0007
# ═══════════════════════════════════════════════════════════════════════

"""
Extract.py - Data Extraction Module

TASKS IMPLEMENTED:
- T0002: Install Airflow, design data models, set up extraction scripts
- T0007: Implement demo script to read/write CSVs

Responsibilities:
- Extract data from CSV source files
- Validate source file existence
- Load data into staging (memory/temp files)
- Track extraction metrics

Source Files (data/raw/dataset/):
- Customers.csv
- Sales.csv  
- Products.csv
- Stores.csv
- Exchange_Rates.csv

Uses: scripts/utils/validation_utils.py for data validation
"""

import pandas as pd
import logging
from typing import Dict, Any, Optional, Tuple
from pathlib import Path
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_RAW = PROJECT_ROOT / "data" / "raw" / "dataset"
DATA_STAGING = PROJECT_ROOT / "data" / "staging"


class DataExtractor:
    """
    TEAM 1 - Data Extraction Handler
    
    Extracts data from CSV files and prepares for transformation.
    """
    
    # Source file definitions with expected columns
    SOURCE_FILES = {
        'customers': {
            'filename': 'Customers.csv',
            'key_column': 'CustomerKey',
            'required_columns': ['CustomerKey', 'Name']
        },
        'sales': {
            'filename': 'Sales.csv',
            'key_column': 'Order Number',
            'required_columns': ['Order Number', 'CustomerKey', 'ProductKey']
        },
        'products': {
            'filename': 'Products.csv',
            'key_column': 'ProductKey',
            'required_columns': ['ProductKey', 'Product Name']
        },
        'stores': {
            'filename': 'Stores.csv',
            'key_column': 'StoreKey',
            'required_columns': ['StoreKey']
        },
        'exchange_rates': {
            'filename': 'Exchange_Rates.csv',
            'key_column': 'Date',
            'required_columns': ['Date', 'Currency']
        }
    }
    
    def __init__(self, data_dir: Optional[Path] = None):
        """
        Initialize extractor
        
        Args:
            data_dir: Path to raw data directory (default: data/raw/dataset)
        """
        self.data_dir = Path(data_dir) if data_dir else DATA_RAW
        self.staging_dir = DATA_STAGING
        self.extracted_data: Dict[str, pd.DataFrame] = {}
        self.extraction_stats: Dict[str, Dict[str, Any]] = {}
        
        # Ensure staging directory exists
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"▶ DataExtractor initialized")
        logger.info(f"   Source: {self.data_dir}")
        logger.info(f"   Staging: {self.staging_dir}")
    
    def validate_source_files(self) -> Dict[str, bool]:
        """
        Validate that all source files exist
        
        Returns:
            Dictionary mapping table name to existence status
        """
        logger.info("▶ Validating source files...")
        
        status = {}
        for table_name, config in self.SOURCE_FILES.items():
            file_path = self.data_dir / config['filename']
            exists = file_path.exists()
            status[table_name] = exists
            
            if exists:
                logger.info(f"   ✅ {config['filename']} found")
            else:
                logger.warning(f"   ❌ {config['filename']} NOT FOUND")
        
        return status
    
    def extract_table(self, table_name: str) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
        """
        Extract a single table from CSV
        
        Args:
            table_name: Name of table to extract (customers, sales, etc.)
        
        Returns:
            Tuple of (DataFrame, stats_dict)
        """
        if table_name not in self.SOURCE_FILES:
            raise ValueError(f"Unknown table: {table_name}")
        
        config = self.SOURCE_FILES[table_name]
        file_path = self.data_dir / config['filename']
        
        stats = {
            'table': table_name,
            'file': config['filename'],
            'start_time': datetime.now(),
            'rows': 0,
            'columns': 0,
            'success': False,
            'error': None
        }
        
        logger.info(f"▶ Extracting {table_name}...")
        
        try:
            # Read CSV
            df = pd.read_csv(file_path)
            
            stats['rows'] = len(df)
            stats['columns'] = len(df.columns)
            stats['success'] = True
            stats['end_time'] = datetime.now()
            stats['duration_seconds'] = (stats['end_time'] - stats['start_time']).total_seconds()
            
            # Validate key column exists
            key_col = config['key_column']
            if key_col not in df.columns:
                logger.warning(f"   ⚠️ Key column '{key_col}' not found in {table_name}")
            
            # Check required columns
            missing_cols = [col for col in config['required_columns'] if col not in df.columns]
            if missing_cols:
                logger.warning(f"   ⚠️ Missing columns: {missing_cols}")
            
            logger.info(f"   ✅ Extracted {stats['rows']:,} rows, {stats['columns']} columns")
            
            return df, stats
            
        except Exception as e:
            stats['success'] = False
            stats['error'] = str(e)
            stats['end_time'] = datetime.now()
            logger.error(f"   ❌ Extraction failed: {e}")
            return None, stats
    
    def extract_all(self) -> Dict[str, pd.DataFrame]:
        """
        Extract all source tables
        
        Returns:
            Dictionary mapping table name to DataFrame
        """
        logger.info("\n" + "="*70)
        logger.info("TEAM 1 - EXTRACT PHASE: Loading All Source Data")
        logger.info("="*70 + "\n")
        
        # Validate files first
        file_status = self.validate_source_files()
        missing_files = [t for t, exists in file_status.items() if not exists]
        
        if missing_files:
            raise FileNotFoundError(f"Missing source files: {missing_files}")
        
        # Extract each table
        for table_name in self.SOURCE_FILES.keys():
            df, stats = self.extract_table(table_name)
            
            if df is not None:
                self.extracted_data[table_name] = df
                self.extraction_stats[table_name] = stats
        
        # Summary
        total_rows = sum(s['rows'] for s in self.extraction_stats.values())
        logger.info("\n" + "-"*50)
        logger.info(f"✅ EXTRACTION COMPLETE: {len(self.extracted_data)} tables, {total_rows:,} total rows")
        logger.info("-"*50 + "\n")
        
        return self.extracted_data
    
    def save_to_staging(self, save_csv: bool = True) -> Dict[str, Path]:
        """
        Save extracted data to staging area
        
        Args:
            save_csv: Whether to save as CSV files
        
        Returns:
            Dictionary mapping table name to staging file path
        """
        logger.info("▶ Saving extracted data to staging...")
        
        staging_files = {}
        
        for table_name, df in self.extracted_data.items():
            if save_csv:
                staging_file = self.staging_dir / f"{table_name}_raw.csv"
                df.to_csv(staging_file, index=False)
                staging_files[table_name] = staging_file
                logger.info(f"   ✅ {table_name} → {staging_file.name}")
        
        return staging_files
    
    def get_extraction_summary(self) -> pd.DataFrame:
        """
        Get summary of extraction results
        
        Returns:
            DataFrame with extraction statistics
        """
        if not self.extraction_stats:
            return pd.DataFrame()
        
        records = []
        for table_name, stats in self.extraction_stats.items():
            records.append({
                'Table': table_name,
                'File': stats['file'],
                'Rows': stats['rows'],
                'Columns': stats['columns'],
                'Duration_Sec': stats.get('duration_seconds', 0),
                'Success': stats['success']
            })
        
        return pd.DataFrame(records)


# ========================================
# Airflow-compatible functions
# ========================================

def extract_all_tables(**context) -> Dict[str, Any]:
    """
    Airflow task callable: Extract all tables
    
    Returns:
        Dictionary with extracted data info
    """
    extractor = DataExtractor()
    
    # Extract all tables
    extracted = extractor.extract_all()
    
    # Save to staging
    staging_files = extractor.save_to_staging()
    
    # Return summary for XCom
    return {
        'tables_extracted': list(extracted.keys()),
        'total_rows': sum(len(df) for df in extracted.values()),
        'staging_files': {k: str(v) for k, v in staging_files.items()},
        'extraction_stats': extractor.extraction_stats
    }


def extract_single_table(table_name: str, **context) -> Dict[str, Any]:
    """
    Airflow task callable: Extract a single table
    
    Args:
        table_name: Name of table to extract
    
    Returns:
        Dictionary with extracted data info
    """
    extractor = DataExtractor()
    df, stats = extractor.extract_table(table_name)
    
    if df is not None:
        extractor.extracted_data[table_name] = df
        extractor.save_to_staging()
    
    return stats


# ========================================
# Main execution
# ========================================

if __name__ == "__main__":
    print("\n" + "="*70)
    print("TEAM 1 - ETL PIPELINE: EXTRACT PHASE TEST")
    print("="*70 + "\n")
    
    # Initialize extractor
    extractor = DataExtractor()
    
    # Extract all tables
    extracted_data = extractor.extract_all()
    
    # Save to staging
    staging_files = extractor.save_to_staging()
    
    # Print summary
    print("\n📊 EXTRACTION SUMMARY:")
    print(extractor.get_extraction_summary().to_string(index=False))
    
    print("\n📁 STAGING FILES:")
    for table, path in staging_files.items():
        print(f"   {table}: {path}")
    
    print("\n✅ Extract phase complete!")
