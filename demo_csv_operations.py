"""
###########  T0007: Demo CSV Operations Script #############
Demonstrates reading and writing CSV files using pandas
Tests data loading from raw data folder
"""

import pandas as pd
import os
from pathlib import Path
from typing import Optional


class CSVOperations:
    """Utility class for CSV operations"""
    
    def __init__(self, data_folder: str = "data/raw/dataset"):
        """Initialize CSV operations with data folder path"""
        self.data_folder = Path(data_folder)
        if not self.data_folder.exists():
            raise FileNotFoundError(f"Data folder not found: {self.data_folder}")
    
    def read_csv(self, filename: str, nrows: Optional[int] = None) -> pd.DataFrame:
        """
        Read a CSV file
        
        Args:
            filename: Name of CSV file in data folder
            nrows: Number of rows to read (None = all)
        
        Returns:
            DataFrame with CSV data
        """
        filepath = self.data_folder / filename
        
        if not filepath.exists():
            raise FileNotFoundError(f"CSV file not found: {filepath}")
        
        print(f"📖 Reading: {filename}")
        df = pd.read_csv(filepath, nrows=nrows)
        print(f"   ✅ Loaded {len(df)} rows, {len(df.columns)} columns")
        return df
    
    def read_all_sources(self) -> dict:
        """Read all 5 source CSV files"""
        sources = {
            'customers': 'Customers.csv',
            'sales': 'Sales.csv',
            'products': 'Products.csv',
            'stores': 'Stores.csv',
            'exchange_rates': 'Exchange_Rates.csv'
        }
        
        data = {}
        print("\n📂 Reading all source files:")
        print("-" * 60)
        
        for name, filename in sources.items():
            try:
                data[name] = self.read_csv(filename)
            except Exception as e:
                print(f"   ❌ Error reading {filename}: {e}")
        
        return data
    
    def write_csv(self, df: pd.DataFrame, filename: str, output_folder: str = "data/processed") -> bool:
        """
        Write DataFrame to CSV
        
        Args:
            df: DataFrame to write
            filename: Output filename
            output_folder: Output folder (relative or absolute path)
        
        Returns:
            True if successful
        """
        try:
            output_path = Path(output_folder)
            output_path.mkdir(parents=True, exist_ok=True)
            
            filepath = output_path / filename
            df.to_csv(filepath, index=False, encoding='utf-8')
            print(f"✅ Written: {filepath} ({len(df)} rows)")
            return True
        except Exception as e:
            print(f"❌ Error writing CSV: {e}")
            return False
    
    def preview_data(self, df: pd.DataFrame, title: str = "", nrows: int = 5):
        """
        Preview DataFrame
        
        Args:
            df: DataFrame to preview
            title: Display title
            nrows: Number of rows to show
        """
        if title:
            print(f"\n📊 {title}")
        print(f"Shape: {df.shape[0]} rows × {df.shape[1]} columns")
        print(f"Columns: {', '.join(df.columns.tolist())}")
        print(f"\nFirst {nrows} rows:")
        print(df.head(nrows))
        print()
    
    def get_data_summary(self, df: pd.DataFrame) -> dict:
        """Get summary statistics about the data"""
        return {
            'rows': len(df),
            'columns': len(df.columns),
            'null_count': df.isnull().sum().to_dict(),
            'column_types': df.dtypes.to_dict(),
            'duplicates': len(df) - len(df.drop_duplicates())
        }


def main():
    """Main demo function"""
    print("=" * 80)
    print("🎯 T0007: CSV Operations Demo - ETL Pipeline Testing")
    print("=" * 80)
    
    try:
        # Initialize CSV operations
        csv_ops = CSVOperations(data_folder="data/raw/dataset")
        
        # 1. Read all source files
        print("\n1️⃣  LOADING ALL SOURCE DATA")
        print("=" * 80)
        all_data = csv_ops.read_all_sources()
        
        # 2. Preview each dataset
        print("\n2️⃣  DATA PREVIEW")
        print("=" * 80)
        for name, df in all_data.items():
            csv_ops.preview_data(df, f"Preview: {name.upper()}")
        
        # 3. Data Summary
        print("\n3️⃣  DATA SUMMARY & QUALITY CHECKS")
        print("=" * 80)
        for name, df in all_data.items():
            summary = csv_ops.get_data_summary(df)
            print(f"\n📊 {name.upper()}:")
            print(f"   Rows: {summary['rows']}")
            print(f"   Columns: {summary['columns']}")
            print(f"   Duplicate rows: {summary['duplicates']}")
            print(f"   Missing values: {sum(summary['null_count'].values())}")
            
            # Show columns with nulls
            null_cols = {k: v for k, v in summary['null_count'].items() if v > 0}
            if null_cols:
                print(f"   Columns with nulls: {null_cols}")
        
        # 4. Test writing to processed folder
        print("\n4️⃣  WRITING SAMPLE DATA TO OUTPUT FOLDER")
        print("=" * 80)
        
        if 'customers' in all_data:
            customers_sample = all_data['customers'].head(10)
            csv_ops.write_csv(
                customers_sample,
                "customers_sample.csv",
                output_folder="data/processed"
            )
        
        if 'sales' in all_data:
            sales_sample = all_data['sales'].head(10)
            csv_ops.write_csv(
                sales_sample,
                "sales_sample.csv",
                output_folder="data/processed"
            )
        
        # 5. Data Profiling
        print("\n5️⃣  DATA PROFILING - CUSTOMERS")
        print("=" * 80)
        
        if 'customers' in all_data:
            customers = all_data['customers']
            print(f"\nColumn Details:")
            for col in customers.columns:
                print(f"  {col}:")
                print(f"    - Type: {customers[col].dtype}")
                print(f"    - Nulls: {customers[col].isnull().sum()}")
                if customers[col].dtype == 'object':
                    print(f"    - Sample values: {customers[col].dropna().head(2).tolist()}")
        
        # 6. Data Profiling - Sales
        print("\n6️⃣  DATA PROFILING - SALES")
        print("=" * 80)
        
        if 'sales' in all_data:
            sales = all_data['sales']
            print(f"\nColumn Details:")
            for col in sales.columns:
                print(f"  {col}:")
                print(f"    - Type: {sales[col].dtype}")
                print(f"    - Nulls: {sales[col].isnull().sum()}")
                if sales[col].dtype == 'object':
                    print(f"    - Sample values: {sales[col].dropna().head(2).tolist()}")
        
        # 7. Summary
        print("\n" + "=" * 80)
        print("✅ CSV DEMO COMPLETED SUCCESSFULLY")
        print("=" * 80)
        print("\n📌 Next Steps:")
        print("   1. Review data quality issues found")
        print("   2. Sample data written to: data/processed/")
        print("   3. Proceed with database schema creation (T0002)")
        print("   4. Configure data cleaning rules (T0008-T0012)")
        
        return all_data
    
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    data = main()
