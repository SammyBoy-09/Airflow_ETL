"""
Database utilities for ETL pipeline
Handles PostgreSQL connections, schema creation, and operations
"""

from sqlalchemy import create_engine, inspect, text, MetaData, Table, Column, Integer, String, Float, DateTime, Date, Boolean, JSON
from sqlalchemy.orm import sessionmaker, Session
from contextlib import contextmanager
from typing import Optional, Dict, Any, List
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages PostgreSQL database operations for ETL pipeline"""
    
    def __init__(self, connection_string: str):
        """
        Initialize database manager
        
        Args:
            connection_string: PostgreSQL connection string
                Example: postgresql+psycopg2://user:password@localhost:5432/dbname
        """
        self.connection_string = connection_string
        self.engine = create_engine(connection_string, echo=False)
        self.Session = sessionmaker(bind=self.engine)
    
    def test_connection(self) -> bool:
        """Test if database connection is working"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("✅ Database connection successful")
            return True
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            return False
    
    @contextmanager
    def get_session(self):
        """Context manager for database sessions"""
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Session error: {e}")
            raise
        finally:
            session.close()
    
    def create_etl_schema(self) -> bool:
        """
        Create all ETL tables in database
        
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.engine.connect() as conn:
                # Input Tables (Raw Data)
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS input_customers (
                        customer_id SERIAL PRIMARY KEY,
                        customer_key VARCHAR(255) UNIQUE,
                        gender VARCHAR(10),
                        name VARCHAR(255),
                        city VARCHAR(100),
                        state_code VARCHAR(5),
                        state VARCHAR(100),
                        zip_code VARCHAR(20),
                        country VARCHAR(100),
                        continent VARCHAR(100),
                        birthday VARCHAR(20),
                        age VARCHAR(10),
                        email VARCHAR(255),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS input_sales (
                        sale_id SERIAL PRIMARY KEY,
                        order_number VARCHAR(255) UNIQUE,
                        line_item VARCHAR(10),
                        order_date VARCHAR(20),
                        delivery_date VARCHAR(20),
                        customer_key VARCHAR(255),
                        store_key VARCHAR(255),
                        product_key VARCHAR(255),
                        quantity INT,
                        currency_code VARCHAR(10),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS input_products (
                        product_id SERIAL PRIMARY KEY,
                        product_key VARCHAR(255) UNIQUE,
                        product_name VARCHAR(255),
                        brand VARCHAR(100),
                        color VARCHAR(50),
                        unit_cost_usd DECIMAL(10,2),
                        unit_price_usd DECIMAL(10,2),
                        subcategory_key VARCHAR(20),
                        subcategory VARCHAR(100),
                        category_key VARCHAR(20),
                        category VARCHAR(100),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS input_stores (
                        store_id SERIAL PRIMARY KEY,
                        store_key VARCHAR(255) UNIQUE,
                        store_name VARCHAR(255),
                        location VARCHAR(255),
                        city VARCHAR(100),
                        country VARCHAR(100),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS input_exchange_rates (
                        rate_id SERIAL PRIMARY KEY,
                        date DATE,
                        currency VARCHAR(10),
                        exchange_rate DECIMAL(10,4),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(date, currency)
                    )
                """))
                
                # Output Tables (Cleaned Data)
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS output_customers_cleaned (
                        customer_id SERIAL PRIMARY KEY,
                        customer_key VARCHAR(255) UNIQUE,
                        name VARCHAR(255),
                        email VARCHAR(255),
                        birthday DATE,
                        age INT,
                        city VARCHAR(100),
                        country VARCHAR(100),
                        cleaned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS output_sales_cleaned (
                        sale_id SERIAL PRIMARY KEY,
                        order_number VARCHAR(255) UNIQUE,
                        order_date DATE,
                        delivery_date DATE,
                        customer_key VARCHAR(255),
                        product_key VARCHAR(255),
                        quantity INT,
                        currency_code VARCHAR(10),
                        price_usd DECIMAL(10,2),
                        total_amount DECIMAL(15,2),
                        delivery_status VARCHAR(20),
                        cleaned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS output_products_cleaned (
                        product_id SERIAL PRIMARY KEY,
                        product_key VARCHAR(255) UNIQUE,
                        product_name VARCHAR(255),
                        brand VARCHAR(100),
                        category VARCHAR(100),
                        unit_price_usd DECIMAL(10,2),
                        cleaned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS output_stores_cleaned (
                        store_id SERIAL PRIMARY KEY,
                        store_key VARCHAR(255) UNIQUE,
                        store_name VARCHAR(255),
                        location VARCHAR(255),
                        city VARCHAR(100),
                        country VARCHAR(100),
                        cleaned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS output_exchange_rates_cleaned (
                        rate_id SERIAL PRIMARY KEY,
                        date DATE,
                        currency VARCHAR(10),
                        exchange_rate DECIMAL(10,4),
                        cleaned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(date, currency)
                    )
                """))
                
                # Summary Tables
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS daily_sales_summary (
                        summary_id SERIAL PRIMARY KEY,
                        date DATE UNIQUE,
                        total_sales DECIMAL(15,2),
                        order_count INT,
                        avg_order_value DECIMAL(10,2),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS product_performance (
                        performance_id SERIAL PRIMARY KEY,
                        product_key VARCHAR(255),
                        product_name VARCHAR(255),
                        total_quantity_sold INT,
                        total_revenue DECIMAL(15,2),
                        avg_price DECIMAL(10,2),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(product_key)
                    )
                """))
                
                # Metadata Tables
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS rejected_records (
                        rejection_id SERIAL PRIMARY KEY,
                        source_table VARCHAR(100),
                        record_id VARCHAR(255),
                        error_type VARCHAR(50),
                        error_details TEXT,
                        rejected_data JSONB,
                        rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS pipeline_runs (
                        run_id SERIAL PRIMARY KEY,
                        dag_id VARCHAR(100),
                        run_date DATE,
                        status VARCHAR(20),
                        records_processed INT,
                        records_rejected INT,
                        start_time TIMESTAMP,
                        end_time TIMESTAMP,
                        duration_seconds INT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                
                conn.commit()
                logger.info("✅ Database schema created successfully")
                return True
        
        except Exception as e:
            logger.error(f"❌ Failed to create schema: {e}")
            return False
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database"""
        try:
            inspector = inspect(self.engine)
            return table_name in inspector.get_table_names()
        except Exception as e:
            logger.error(f"Error checking table existence: {e}")
            return False
    
    def get_table_row_count(self, table_name: str) -> Optional[int]:
        """Get row count for a table"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                return result.scalar()
        except Exception as e:
            logger.error(f"Error getting row count: {e}")
            return None
    
    def insert_data(self, table_name: str, data: List[Dict[str, Any]]) -> bool:
        """Insert data into a table"""
        try:
            if not data:
                logger.warning(f"No data to insert into {table_name}")
                return True
            
            with self.engine.connect() as conn:
                # Use raw SQL insert for simplicity
                columns = ', '.join(data[0].keys())
                placeholders = ', '.join([f":{k}" for k in data[0].keys()])
                
                insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                
                for record in data:
                    conn.execute(text(insert_sql), record)
                
                conn.commit()
                logger.info(f"✅ Inserted {len(data)} records into {table_name}")
                return True
        
        except Exception as e:
            logger.error(f"❌ Error inserting data: {e}")
            return False
    
    def log_rejection(self, source_table: str, record_id: str, error_type: str, 
                     error_details: str, rejected_data: Dict[str, Any]) -> bool:
        """Log a rejected record"""
        try:
            import json
            with self.engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO rejected_records 
                    (source_table, record_id, error_type, error_details, rejected_data)
                    VALUES (:source_table, :record_id, :error_type, :error_details, :rejected_data)
                """), {
                    'source_table': source_table,
                    'record_id': record_id,
                    'error_type': error_type,
                    'error_details': error_details,
                    'rejected_data': json.dumps(rejected_data)
                })
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Error logging rejection: {e}")
            return False
    
    def close(self):
        """Close database connection"""
        self.engine.dispose()
        logger.info("Database connection closed")
