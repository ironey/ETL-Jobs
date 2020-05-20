import csv
import sqlite3
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
import pandas as pd
import numpy as np

# Set up logging with more detailed configuration
logging.basicConfig(
    filename='data/etl.log',
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('ETL')

class ETLMetrics:
    def __init__(self):
        self.start_time = time.time()
        self.processed_records = 0
        self.failed_records = 0
        
    def log_metrics(self):
        duration = time.time() - self.start_time
        logger.info(f"""
        ETL Metrics:
        - Duration: {duration:.2f} seconds
        - Processed Records: {self.processed_records}
        - Failed Records: {self.failed_records}
        - Success Rate: {(self.processed_records/(self.processed_records + self.failed_records))*100:.2f}%
        """)

class DataValidator:
    @staticmethod
    def validate_row(row: Dict) -> Optional[Dict]:
        try:
            # Enhanced validation rules
            if not all(key in row for key in ['name', 'age', 'email']):
                raise ValueError("Missing required fields")
            
            if not row['name'].strip():
                raise ValueError("Name cannot be empty")
            
            # Email validation
            if '@' not in row['email']:
                raise ValueError("Invalid email format")
            
            # Age validation
            try:
                age = int(row['age'])
                if not 0 <= age <= 150:
                    raise ValueError("Age must be between 0 and 150")
            except ValueError:
                raise ValueError("Invalid age format")
            
            return row
        except ValueError as e:
            logger.error(f"Validation error: {str(e)} - Row: {row}")
            return None

def process_chunk(chunk: pd.DataFrame) -> List[Dict]:
    validator = DataValidator()
    transformed = []
    
    for _, row in chunk.iterrows():
        row_dict = row.to_dict()
        validated_row = validator.validate_row(row_dict)
        if validated_row:
            validated_row['name'] = validated_row['name'].upper()
            validated_row['age'] = int(validated_row['age'])
            validated_row['processed_at'] = datetime.now().isoformat()
            transformed.append(validated_row)
    
    return transformed

def extract(file_path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(file_path)
    except Exception as e:
        logger.error(f"Extract error: {str(e)}")
        raise

def transform_parallel(data: pd.DataFrame, num_workers: int = 3) -> List[Dict]:
    # Split data into chunks for parallel processing
    chunk_size = len(data) // num_workers
    chunks = np.array_split(data, num_workers)
    
    transformed_data = []
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        # Submit all chunks for parallel processing
        future_to_chunk = {executor.submit(process_chunk, chunk): i 
                          for i, chunk in enumerate(chunks)}
        
        # Collect results as they complete
        for future in as_completed(future_to_chunk):
            chunk_id = future_to_chunk[future]
            try:
                transformed_data.extend(future.result())
                logger.info(f"Chunk {chunk_id} processed successfully")
            except Exception as e:
                logger.error(f"Error processing chunk {chunk_id}: {str(e)}")
    
    return transformed_data

def load(data: List[Dict]) -> None:
    conn = sqlite3.connect('data/database.db')
    cursor = conn.cursor()
    
    try:
        # Enhanced table schema
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                age INTEGER NOT NULL,
                email TEXT NOT NULL,
                processed_at TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Batch insert for better performance
        cursor.executemany(
            'INSERT INTO users (name, age, email, processed_at) VALUES (?, ?, ?, ?)',
            [(row['name'], row['age'], row['email'], row['processed_at']) for row in data]
        )
        
        conn.commit()
        logger.info(f"Successfully loaded {len(data)} records")
    except Exception as e:
        conn.rollback()
        logger.error(f"Load error: {str(e)}")
        raise
    finally:
        conn.close()

if __name__ == '__main__':
    metrics = ETLMetrics()
    
    try:
        logger.info("Starting ETL process...")
        
        # Process all CSV files in data directory
        data_dir = Path('data')
        all_data = []
        
        for csv_file in data_dir.glob('*.csv'):
            if csv_file.name != 'output.csv':  # Skip output files
                logger.info(f"Processing {csv_file}")
                df = extract(str(csv_file))
                all_data.append(df)
        
        if all_data:
            combined_data = pd.concat(all_data, ignore_index=True)
            logger.info(f"Total records to process: {len(combined_data)}")
            
            transformed_data = transform_parallel(combined_data)
            metrics.processed_records = len(transformed_data)
            metrics.failed_records = len(combined_data) - len(transformed_data)
            
            load(transformed_data)
            logger.info("ETL process completed successfully!")
        else:
            logger.warning("No CSV files found to process")
        
        metrics.log_metrics()
    except Exception as e:
        logger.error(f"ETL process failed: {str(e)}")
        raise
