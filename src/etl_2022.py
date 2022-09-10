import csv
import sqlite3
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
import pandas as pd
import numpy as np
from dataclasses import dataclass
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import json

# Configure logging
logging.basicConfig(
    filename='data/etl.log',
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('ETL')

@dataclass
class DataQualityMetrics:
    total_records: int = 0
    valid_records: int = 0
    null_counts: Dict[str, int] = None
    duplicates: int = 0
    outliers: Dict[str, List] = None
    processing_time: float = 0.0

    def to_dict(self) -> Dict:
        return {
            'timestamp': datetime.now().isoformat(),
            'total_records': self.total_records,
            'valid_records': self.valid_records,
            'completion_rate': (self.valid_records / self.total_records * 100) if self.total_records else 0,
            'null_counts': self.null_counts,
            'duplicates': self.duplicates,
            'outliers': self.outliers,
            'processing_time': self.processing_time
        }

class DataQualityChecker:
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.metrics = DataQualityMetrics()
        self.metrics.total_records = len(df)

    def check_nulls(self) -> Dict[str, int]:
        """Check for null values in each column"""
        return self.df.isnull().sum().to_dict()

    def check_duplicates(self) -> int:
        """Check for duplicate records"""
        return self.df.duplicated().sum()

    def detect_outliers(self, numeric_columns: List[str]) -> Dict[str, List]:
        """Detect outliers using IQR method"""
        outliers = {}
        for col in numeric_columns:
            if col in self.df.columns and pd.api.types.is_numeric_dtype(self.df[col]):
                Q1 = self.df[col].quantile(0.25)
                Q3 = self.df[col].quantile(0.75)
                IQR = Q3 - Q1
                outliers[col] = self.df[
                    (self.df[col] < (Q1 - 1.5 * IQR)) | 
                    (self.df[col] > (Q3 + 1.5 * IQR))
                ][col].tolist()
        return outliers

    def run_quality_checks(self) -> DataQualityMetrics:
        """Run all data quality checks"""
        self.metrics.null_counts = self.check_nulls()
        self.metrics.duplicates = self.check_duplicates()
        self.metrics.outliers = self.detect_outliers(['age'])
        return self.metrics

class DataProfiler:
    @staticmethod
    def profile_data(df: pd.DataFrame) -> Dict:
        """Generate a data profile report"""
        profile = {
            'record_count': len(df),
            'column_stats': {},
            'timestamp': datetime.now().isoformat()
        }

        for column in df.columns:
            stats = {
                'data_type': str(df[column].dtype),
                'unique_values': df[column].nunique(),
                'missing_values': df[column].isnull().sum(),
            }
            
            if pd.api.types.is_numeric_dtype(df[column]):
                stats.update({
                    'mean': df[column].mean(),
                    'median': df[column].median(),
                    'std': df[column].std(),
                    'min': df[column].min(),
                    'max': df[column].max()
                })
            
            profile['column_stats'][column] = stats
        
        return profile

class AlertManager:
    @staticmethod
    def should_alert(metrics: DataQualityMetrics) -> bool:
        """Check if metrics warrant an alert"""
        alert_conditions = [
            metrics.valid_records / metrics.total_records < 0.9,  # Less than 90% valid records
            any(count > 100 for count in metrics.null_counts.values()),  # Too many nulls
            metrics.duplicates > 50,  # Too many duplicates
            any(len(outliers) > 10 for outliers in metrics.outliers.values())  # Too many outliers
        ]
        return any(alert_conditions)

    @staticmethod
    def send_alert(metrics: DataQualityMetrics):
        """Send alert email with quality metrics"""
        msg = MIMEMultipart()
        msg['Subject'] = 'ETL Data Quality Alert'
        msg['From'] = 'etl-monitor@example.com'
        msg['To'] = 'admin@example.com'
        
        body = f"""
        Data Quality Alert:
        - Total Records: {metrics.total_records}
        - Valid Records: {metrics.valid_records}
        - Completion Rate: {metrics.valid_records/metrics.total_records*100:.2f}%
        - Null Counts: {metrics.null_counts}
        - Duplicates: {metrics.duplicates}
        - Processing Time: {metrics.processing_time:.2f} seconds
        """
        
        msg.attach(MIMEText(body, 'plain'))
        
        # In production, replace with actual SMTP server details
        logger.info(f"Would send alert:\n{body}")

def extract(file_path: str) -> Tuple[pd.DataFrame, DataQualityMetrics]:
    """Extract data with quality checks"""
    start_time = time.time()
    
    try:
        df = pd.read_csv(file_path)
        
        # Run data quality checks
        checker = DataQualityChecker(df)
        metrics = checker.run_quality_checks()
        
        # Generate data profile
        profile = DataProfiler.profile_data(df)
        
        # Save quality metrics and profile
        with open(f'data/quality_metrics_{Path(file_path).stem}.json', 'w') as f:
            json.dump(metrics.to_dict(), f, indent=2)
        
        with open(f'data/data_profile_{Path(file_path).stem}.json', 'w') as f:
            json.dump(profile, f, indent=2)
        
        metrics.processing_time = time.time() - start_time
        return df, metrics
    
    except Exception as e:
        logger.error(f"Extract error: {str(e)}")
        raise

def transform_with_quality(df: pd.DataFrame) -> pd.DataFrame:
    """Transform data with additional quality checks"""
    try:
        # Remove duplicates
        df = df.drop_duplicates()
        
        # Handle missing values
        df['name'] = df['name'].fillna('UNKNOWN')
        df['age'] = df['age'].fillna(df['age'].mean())
        df['email'] = df['email'].fillna('unknown@example.com')
        
        # Transform data
        df['name'] = df['name'].str.upper()
        df['age'] = df['age'].astype(int)
        df['processed_at'] = datetime.now().isoformat()
        
        return df
    except Exception as e:
        logger.error(f"Transform error: {str(e)}")
        raise

def load_with_monitoring(df: pd.DataFrame) -> None:
    """Load data with performance monitoring"""
    start_time = time.time()
    conn = sqlite3.connect('data/database.db')
    
    try:
        # Create table with quality tracking columns
        conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                age INTEGER NOT NULL,
                email TEXT NOT NULL,
                processed_at TEXT NOT NULL,
                quality_score REAL,
                batch_id TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Calculate quality score (simple example)
        df['quality_score'] = 1.0 - (df.isnull().sum(axis=1) / len(df.columns))
        df['batch_id'] = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Load data
        df.to_sql('users', conn, if_exists='append', index=False)
        
        # Log performance metrics
        duration = time.time() - start_time
        logger.info(f"Load completed in {duration:.2f} seconds for {len(df)} records")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Load error: {str(e)}")
        raise
    finally:
        conn.close()

if __name__ == '__main__':
    try:
        logger.info("Starting ETL process with quality checks...")
        
        # Process all CSV files
        data_dir = Path('data')
        all_data = []
        total_metrics = DataQualityMetrics()
        
        for csv_file in data_dir.glob('*.csv'):
            if csv_file.name.startswith('input_'):
                logger.info(f"Processing {csv_file}")
                df, metrics = extract(str(csv_file))
                
                # Update total metrics
                total_metrics.total_records += metrics.total_records
                total_metrics.valid_records += metrics.valid_records
                
                # Transform and store data
                transformed_df = transform_with_quality(df)
                all_data.append(transformed_df)
        
        if all_data:
            # Combine all transformed data
            final_df = pd.concat(all_data, ignore_index=True)
            
            # Load data
            load_with_monitoring(final_df)
            
            # Check if should alert
            if AlertManager.should_alert(total_metrics):
                AlertManager.send_alert(total_metrics)
            
            logger.info("ETL process completed successfully!")
        else:
            logger.warning("No input CSV files found to process")
            
    except Exception as e:
        logger.error(f"ETL process failed: {str(e)}")
        raise
