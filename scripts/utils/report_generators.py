############# TEAM 1 - Phase 4: Report Generation Utilities #############

"""
Report Generation for ETL Pipeline
Generates 8 comprehensive reports from cleaned data

Reports:
1. Customer Summary
2. Product Performance
3. Order Status
4. Sales Trends Daily
5. Data Quality Scorecard
6. Customer Segmentation (RFM)
7. Store Performance
8. Anomaly Detection
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from typing import Dict, Any, Optional, Tuple
from pathlib import Path

logger = logging.getLogger(__name__)


class CustomerSummaryReport:
    """
    TEAM 1 - Customer Summary Report
    Aggregates customer-level metrics
    """
    
    @staticmethod
    def generate(customers_df: pd.DataFrame, sales_df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate customer summary report
        
        Columns:
        - CustomerKey, Total_Orders, Total_Quantity, Total_Spend_USD
        - Avg_Order_Value, First_Order_Date, Last_Order_Date
        - Customer_Tenure_Days, Recency_Days, Order_Frequency
        - Is_Active, Customer_Segment, Avg_Delivery_Time
        """
        logger.info("▶ Generating Customer Summary Report")
        
        reference_date = datetime.now()
        
        # Ensure datetime
        sales_df['Order Date'] = pd.to_datetime(sales_df['Order Date'])
        if 'Delivery Date' in sales_df.columns:
            sales_df['Delivery Date'] = pd.to_datetime(sales_df['Delivery Date'])
        
        # Aggregate by customer
        customer_agg = sales_df.groupby('CustomerKey').agg({
            'Order Number': 'count',  # Total orders
            'Quantity': 'sum',  # Total quantity
            'Total_Amount_USD': 'sum',  # Total spend
            'Order Date': ['min', 'max']  # First and last order
        }).reset_index()
        
        customer_agg.columns = [
            'CustomerKey',
            'Total_Orders',
            'Total_Quantity',
            'Total_Spend_USD',
            'First_Order_Date',
            'Last_Order_Date'
        ]
        
        # Calculate derived metrics
        customer_agg['Avg_Order_Value'] = customer_agg['Total_Spend_USD'] / customer_agg['Total_Orders']
        customer_agg['Customer_Tenure_Days'] = (reference_date - customer_agg['First_Order_Date']).dt.days
        customer_agg['Recency_Days'] = (reference_date - customer_agg['Last_Order_Date']).dt.days
        customer_agg['Order_Frequency'] = customer_agg['Total_Orders'] / (customer_agg['Customer_Tenure_Days'] / 30.0 + 1)
        customer_agg['Is_Active'] = (customer_agg['Recency_Days'] <= 90).astype(int)
        
        # Customer segmentation (simple rule-based)
        def segment_customer(row):
            if row['Recency_Days'] > 365:
                return 'Inactive'
            elif row['Total_Orders'] == 1:
                return 'New'
            elif row['Total_Spend_USD'] > 5000 and row['Total_Orders'] > 10:
                return 'Gold'
            elif row['Total_Spend_USD'] > 2000 and row['Total_Orders'] > 5:
                return 'Silver'
            elif row['Recency_Days'] > 180 and row['Total_Orders'] > 5:
                return 'At_Risk'
            else:
                return 'Bronze'
        
        customer_agg['Customer_Segment'] = customer_agg.apply(segment_customer, axis=1)
        
        # Average delivery time
        if 'Delivery Date' in sales_df.columns:
            delivery_time = sales_df[sales_df['Delivery Date'].notna()].copy()
            delivery_time['Delivery_Days'] = (delivery_time['Delivery Date'] - delivery_time['Order Date']).dt.days
            avg_delivery = delivery_time.groupby('CustomerKey')['Delivery_Days'].mean().reset_index()
            avg_delivery.columns = ['CustomerKey', 'Avg_Delivery_Time']
            customer_agg = customer_agg.merge(avg_delivery, on='CustomerKey', how='left')
        
        logger.info(f"✅ Customer summary generated: {len(customer_agg):,} customers")
        
        return customer_agg


class ProductPerformanceReport:
    """
    TEAM 1 - Product Performance Report
    Analyzes product sales metrics
    """
    
    @staticmethod
    def generate(products_df: pd.DataFrame, sales_df: pd.DataFrame) -> pd.DataFrame:
        """Generate product performance report"""
        logger.info("▶ Generating Product Performance Report")
        
        # Aggregate by product
        product_agg = sales_df.groupby('ProductKey').agg({
            'Quantity': 'sum',
            'Order Number': 'count',
            'Total_Amount_USD': 'sum',
            'Order Date': 'max'
        }).reset_index()
        
        product_agg.columns = [
            'ProductKey',
            'Total_Units_Sold',
            'Total_Orders',
            'Total_Revenue_USD',
            'Last_Sold_Date'
        ]
        
        # Merge with product details
        product_report = products_df.merge(product_agg, on='ProductKey', how='left')
        product_report['Total_Units_Sold'] = product_report['Total_Units_Sold'].fillna(0)
        product_report['Total_Orders'] = product_report['Total_Orders'].fillna(0)
        product_report['Total_Revenue_USD'] = product_report['Total_Revenue_USD'].fillna(0)
        
        # Calculate metrics
        product_report['Avg_Order_Quantity'] = np.where(
            product_report['Total_Orders'] > 0,
            product_report['Total_Units_Sold'] / product_report['Total_Orders'],
            0
        )
        
        product_report['Sales_Rank'] = product_report['Total_Revenue_USD'].rank(ascending=False, method='min').astype(int)
        product_report['Is_Top_Seller'] = (product_report['Sales_Rank'] <= len(product_report) * 0.1).astype(int)
        
        # Days since last sale
        reference_date = datetime.now()
        product_report['Last_Sold_Date'] = pd.to_datetime(product_report['Last_Sold_Date'])
        product_report['Days_Since_Last_Sale'] = (reference_date - product_report['Last_Sold_Date']).dt.days
        product_report['Days_Since_Last_Sale'] = product_report['Days_Since_Last_Sale'].fillna(999)
        
        logger.info(f"✅ Product performance generated: {len(product_report):,} products")
        
        return product_report


class OrderStatusReport:
    """
    TEAM 1 - Order Status Report
    Summarizes delivery status distribution
    """
    
    @staticmethod
    def generate(sales_df: pd.DataFrame) -> pd.DataFrame:
        """Generate order status summary"""
        logger.info("▶ Generating Order Status Report")
        
        status_agg = sales_df.groupby('Delivery_Status').agg({
            'Order Number': 'count',
            'Total_Amount_USD': 'sum',
            'CustomerKey': 'nunique'
        }).reset_index()
        
        status_agg.columns = [
            'Delivery_Status',
            'Order_Count',
            'Total_Value_USD',
            'Customers_Affected'
        ]
        
        status_agg['Pct_Of_Total_Orders'] = (status_agg['Order_Count'] / status_agg['Order_Count'].sum() * 100).round(2)
        
        # Calculate average days pending for non-delivered
        non_delivered = sales_df[sales_df['Delivery_Status'] != 'Delivered'].copy()
        if len(non_delivered) > 0:
            reference_date = datetime.now()
            non_delivered['Days_Pending'] = (reference_date - non_delivered['Order Date']).dt.days
            avg_pending = non_delivered.groupby('Delivery_Status')['Days_Pending'].mean().reset_index()
            avg_pending.columns = ['Delivery_Status', 'Avg_Days_Pending']
            status_agg = status_agg.merge(avg_pending, on='Delivery_Status', how='left')
        
        logger.info(f"✅ Order status report generated: {len(status_agg)} statuses")
        
        return status_agg


class SalesTrendsReport:
    """
    TEAM 1 - Sales Trends Daily Report
    Daily sales metrics for trend analysis
    """
    
    @staticmethod
    def generate(sales_df: pd.DataFrame) -> pd.DataFrame:
        """Generate daily sales trends"""
        logger.info("▶ Generating Sales Trends (Daily) Report")
        
        sales_df['Order Date'] = pd.to_datetime(sales_df['Order Date'])
        sales_df['Date'] = sales_df['Order Date'].dt.date
        
        # Aggregate by date
        daily_sales = sales_df.groupby('Date').agg({
            'Order Number': 'count',
            'Total_Amount_USD': 'sum',
            'Quantity': 'sum',
            'CustomerKey': 'nunique'
        }).reset_index()
        
        daily_sales.columns = [
            'Date',
            'Total_Orders',
            'Total_Revenue_USD',
            'Total_Quantity',
            'Unique_Customers'
        ]
        
        daily_sales['Avg_Order_Value'] = daily_sales['Total_Revenue_USD'] / daily_sales['Total_Orders']
        
        # Add date features
        daily_sales['Date'] = pd.to_datetime(daily_sales['Date'])
        daily_sales['Day_Of_Week'] = daily_sales['Date'].dt.dayofweek
        daily_sales['Is_Weekend'] = (daily_sales['Day_Of_Week'] >= 5).astype(int)
        daily_sales['Month'] = daily_sales['Date'].dt.month_name()
        daily_sales['Quarter'] = daily_sales['Date'].dt.quarter
        
        logger.info(f"✅ Sales trends generated: {len(daily_sales)} days")
        
        return daily_sales


class DataQualityScorecard:
    """
    TEAM 1 - Data Quality Scorecard
    Summarizes data quality metrics for all tables
    """
    
    @staticmethod
    def generate(cleaning_stats: Dict[str, Dict[str, Any]]) -> pd.DataFrame:
        """Generate data quality scorecard from cleaning stats"""
        logger.info("▶ Generating Data Quality Scorecard")
        
        records = []
        
        for table_name, stats in cleaning_stats.items():
            records.append({
                'Table': table_name.title(),
                'Total_Rows': stats.get('output_rows', 0),
                'Duplicates_Removed': stats.get('duplicates_removed', 0),
                'Null_Values_Before': stats.get('input_rows', 0) - stats.get('output_rows', 0),
                'Null_Values_After': 0,  # Assuming all filled
                'Dates_Standardized': stats.get('dates_standardized', 0),
                'Special_Fixes': stats.get('invalid_emails_fixed', 0) + stats.get('missing_ages_filled', 0),
                'Validation_Status': 'PASS' if stats.get('output_rows', 0) > 0 else 'FAIL'
            })
        
        scorecard = pd.DataFrame(records)
        
        logger.info(f"✅ Data quality scorecard generated: {len(scorecard)} tables")
        
        return scorecard


class CustomerSegmentationReport:
    """
    TEAM 1 - Customer Segmentation (RFM)
    RFM-based customer segmentation
    """
    
    @staticmethod
    def generate(customer_summary_df: pd.DataFrame) -> pd.DataFrame:
        """Generate RFM segmentation"""
        logger.info("▶ Generating Customer Segmentation (RFM) Report")
        
        # Calculate RFM scores (1-5)
        customer_summary_df['R_Score'] = pd.qcut(
            customer_summary_df['Recency_Days'],
            q=5,
            labels=[5, 4, 3, 2, 1],
            duplicates='drop'
        ).astype(int)
        
        customer_summary_df['F_Score'] = pd.qcut(
            customer_summary_df['Total_Orders'],
            q=5,
            labels=[1, 2, 3, 4, 5],
            duplicates='drop'
        ).astype(int)
        
        customer_summary_df['M_Score'] = pd.qcut(
            customer_summary_df['Total_Spend_USD'],
            q=5,
            labels=[1, 2, 3, 4, 5],
            duplicates='drop'
        ).astype(int)
        
        customer_summary_df['RFM_Score'] = (
            customer_summary_df['R_Score'] * 100 +
            customer_summary_df['F_Score'] * 10 +
            customer_summary_df['M_Score']
        )
        
        # Segment distribution
        segment_summary = customer_summary_df.groupby('Customer_Segment').agg({
            'CustomerKey': 'count',
            'Total_Spend_USD': 'sum',
            'Total_Orders': 'sum',
            'Recency_Days': 'mean'
        }).reset_index()
        
        segment_summary.columns = [
            'Segment',
            'Customer_Count',
            'Total_Revenue_USD',
            'Total_Orders',
            'Avg_Recency_Days'
        ]
        
        logger.info(f"✅ Customer segmentation generated: {len(segment_summary)} segments")
        
        return segment_summary


class StorePerformanceReport:
    """
    TEAM 1 - Store Performance Report
    Store-level sales metrics
    """
    
    @staticmethod
    def generate(stores_df: pd.DataFrame, sales_df: pd.DataFrame) -> pd.DataFrame:
        """Generate store performance report"""
        logger.info("▶ Generating Store Performance Report")
        
        # Aggregate by store
        store_agg = sales_df.groupby('StoreKey').agg({
            'Order Number': 'count',
            'Total_Amount_USD': 'sum',
            'CustomerKey': 'nunique'
        }).reset_index()
        
        store_agg.columns = [
            'StoreKey',
            'Total_Orders',
            'Total_Revenue_USD',
            'Total_Customers'
        ]
        
        store_agg['Avg_Order_Value'] = store_agg['Total_Revenue_USD'] / store_agg['Total_Orders']
        
        # Merge with store details
        store_report = stores_df.merge(store_agg, on='StoreKey', how='left')
        store_report[['Total_Orders', 'Total_Revenue_USD', 'Total_Customers']] = store_report[
            ['Total_Orders', 'Total_Revenue_USD', 'Total_Customers']
        ].fillna(0)
        
        # Delivery success rate (if data available)
        if 'Delivery_Status' in sales_df.columns:
            delivered = sales_df[sales_df['Delivery_Status'] == 'Delivered'].groupby('StoreKey').size().reset_index(name='Delivered_Count')
            store_report = store_report.merge(delivered, on='StoreKey', how='left')
            store_report['Delivered_Count'] = store_report['Delivered_Count'].fillna(0)
            store_report['Delivery_Success_Rate'] = (store_report['Delivered_Count'] / store_report['Total_Orders'] * 100).fillna(0).round(2)
        
        logger.info(f"✅ Store performance generated: {len(store_report):,} stores")
        
        return store_report


class AnomalyDetectionReport:
    """
    TEAM 1 - Anomaly Detection Report
    Detects unusual patterns in orders
    """
    
    @staticmethod
    def generate(sales_df: pd.DataFrame) -> pd.DataFrame:
        """Detect anomalies in sales data"""
        logger.info("▶ Generating Anomaly Detection Report")
        
        anomalies = []
        
        # 1. Large order anomalies (Quantity > 3 std dev)
        qty_mean = sales_df['Quantity'].mean()
        qty_std = sales_df['Quantity'].std()
        large_orders = sales_df[sales_df['Quantity'] > qty_mean + 3 * qty_std]
        
        for _, row in large_orders.iterrows():
            anomalies.append({
                'Order_Number': row['Order Number'],
                'Anomaly_Type': 'Large_Order',
                'Description': f"Quantity {row['Quantity']} exceeds 3 std dev from mean",
                'Severity': 'High',
                'Value': row['Quantity']
            })
        
        # 2. Delayed delivery anomalies (>60 days)
        if 'Delivery Date' in sales_df.columns:
            delivered = sales_df[sales_df['Delivery Date'].notna()].copy()
            delivered['Delivery_Days'] = (delivered['Delivery Date'] - delivered['Order Date']).dt.days
            delayed = delivered[delivered['Delivery_Days'] > 60]
            
            for _, row in delayed.head(100).iterrows():  # Limit to 100
                anomalies.append({
                    'Order_Number': row['Order Number'],
                    'Anomaly_Type': 'Delayed_Delivery',
                    'Description': f"{int(row['Delivery_Days'])} days to deliver",
                    'Severity': 'Medium',
                    'Value': row['Delivery_Days']
                })
        
        # 3. Price spike anomalies (if available)
        if 'Total_Amount_USD' in sales_df.columns:
            amount_mean = sales_df['Total_Amount_USD'].mean()
            amount_std = sales_df['Total_Amount_USD'].std()
            price_spikes = sales_df[sales_df['Total_Amount_USD'] > amount_mean + 3 * amount_std]
            
            for _, row in price_spikes.head(50).iterrows():
                anomalies.append({
                    'Order_Number': row['Order Number'],
                    'Anomaly_Type': 'Price_Spike',
                    'Description': f"Amount ${row['Total_Amount_USD']:.2f} exceeds 3 std dev",
                    'Severity': 'High',
                    'Value': row['Total_Amount_USD']
                })
        
        anomaly_report = pd.DataFrame(anomalies)
        
        logger.info(f"✅ Anomaly detection complete: {len(anomaly_report)} anomalies detected")
        
        return anomaly_report


# ========================================
# TEAM 1 - Report Generation Orchestrator
# ========================================

class ReportOrchestrator:
    """
    Generate all 8 reports in one call
    """
    
    @staticmethod
    def generate_all_reports(cleaned_tables: Dict[str, pd.DataFrame],
                            cleaning_stats: Dict[str, Dict[str, Any]]) -> Dict[str, pd.DataFrame]:
        """
        Generate all reports
        
        Args:
            cleaned_tables: Dictionary with cleaned DataFrames
            cleaning_stats: Dictionary with cleaning statistics
        
        Returns:
            Dictionary with all report DataFrames
        """
        logger.info("\n" + "="*70)
        logger.info("TEAM 1 - REPORT GENERATION PIPELINE")
        logger.info("="*70 + "\n")
        
        reports = {}
        
        try:
            customers_df = cleaned_tables['customers']
            sales_df = cleaned_tables['sales']
            products_df = cleaned_tables['products']
            stores_df = cleaned_tables['stores']
            
            # Generate all 8 reports
            reports['customer_summary'] = CustomerSummaryReport.generate(customers_df, sales_df)
            reports['product_performance'] = ProductPerformanceReport.generate(products_df, sales_df)
            reports['order_status'] = OrderStatusReport.generate(sales_df)
            reports['sales_trends_daily'] = SalesTrendsReport.generate(sales_df)
            reports['data_quality_scorecard'] = DataQualityScorecard.generate(cleaning_stats)
            reports['customer_segmentation'] = CustomerSegmentationReport.generate(reports['customer_summary'])
            reports['store_performance'] = StorePerformanceReport.generate(stores_df, sales_df)
            reports['anomaly_detection'] = AnomalyDetectionReport.generate(sales_df)
            
            logger.info("="*70)
            logger.info(f"✅ ALL {len(reports)} REPORTS GENERATED SUCCESSFULLY")
            logger.info("="*70 + "\n")
            
        except Exception as e:
            logger.error(f"❌ Report generation failed: {e}")
            raise
        
        return reports
