import csv
import sqlite3
import logging
from datetime import datetime
from typing import Dict, List, Optional

# Set up logging
logging.basicConfig(
    filename='data/etl_errors.log',
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class DataValidator:
    @staticmethod
    def validate_row(row: Dict) -> Optional[Dict]:
        try:
            # Validate required fields
            if not all(key in row for key in ['name', 'age']):
                raise ValueError("Missing required fields")
            
            # Validate data types
            if not row['name'].strip():
                raise ValueError("Name cannot be empty")
            
            try:
                age = int(row['age'])
                if not 0 <= age <= 150:
                    raise ValueError("Age must be between 0 and 150")
            except ValueError:
                raise ValueError("Invalid age format")
            
            return row
        except ValueError as e:
            logging.error(f"Validation error: {str(e)} - Row: {row}")
            return None

def extract() -> List[Dict]:
    data = []
    try:
        with open('data/input.csv', 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                data.append(row)
        return data
    except Exception as e:
        logging.error(f"Extract error: {str(e)}")
        raise

def transform(data: List[Dict]) -> List[Dict]:
    transformed = []
    validator = DataValidator()
    
    for row in data:
        validated_row = validator.validate_row(row)
        if validated_row:
            # Transform valid data
            validated_row['name'] = validated_row['name'].upper()
            validated_row['age'] = int(validated_row['age'])
            validated_row['processed_at'] = datetime.now().isoformat()
            transformed.append(validated_row)
    
    return transformed

def load(data: List[Dict]) -> None:
    conn = sqlite3.connect('data/database.db')
    cursor = conn.cursor()
    
    try:
        # Create table with additional columns
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                name TEXT NOT NULL,
                age INTEGER NOT NULL,
                processed_at TEXT NOT NULL
            )
        ''')
        
        # Insert data
        for row in data:
            cursor.execute(
                'INSERT INTO users (name, age, processed_at) VALUES (?, ?, ?)',
                (row['name'], row['age'], row['processed_at'])
            )
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        logging.error(f"Load error: {str(e)}")
        raise
    finally:
        conn.close()

if __name__ == '__main__':
    try:
        print("Starting ETL process...")
        data = extract()
        print(f"Extracted {len(data)} rows")
        
        transformed_data = transform(data)
        print(f"Transformed {len(transformed_data)} valid rows")
        
        load(transformed_data)
        print("ETL process completed successfully!")
    except Exception as e:
        print(f"ETL process failed: {str(e)}")
        logging.error(f"ETL process failed: {str(e)}")
