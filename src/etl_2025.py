import asyncio
import aioboto3
import aiohttp
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any
from datetime import datetime
import logging
import json
from pathlib import Path
from dataclasses import dataclass
import sqlite3
import aiomysql
from concurrent.futures import ProcessPoolExecutor
import os
from functools import partial
import boto3
from botocore.exceptions import ClientError
import yaml
from prometheus_client import Counter, Histogram, start_http_server

# Configure logging with structured output
import structlog
logger = structlog.get_logger()

# Metrics
PROCESSED_RECORDS = Counter('etl_processed_records_total', 'Number of records processed')
PROCESSING_TIME = Histogram('etl_processing_seconds', 'Time spent processing records')
ERROR_COUNT = Counter('etl_errors_total', 'Number of processing errors')

# Load configuration
with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

@dataclass
class CloudConfig:
    """Cloud configuration settings"""
    aws_region: str = config['aws']['region']
    s3_bucket: str = config['aws']['s3_bucket']
    sqs_queue_url: str = config['aws']['sqs_queue_url']
    db_host: str = config['database']['host']
    db_name: str = config['database']['name']
    db_user: str = config['database']['user']
    db_password: str = config['database']['password']

class CloudStorageManager:
    """Manages interactions with cloud storage"""
    
    def __init__(self):
        self.session = aioboto3.Session()
        self.config = CloudConfig()

    async def upload_file(self, local_path: str, s3_key: str) -> bool:
        """Upload file to S3 with versioning"""
        try:
            async with self.session.client('s3') as s3:
                await s3.upload_file(
                    local_path, 
                    self.config.s3_bucket,
                    s3_key,
                    ExtraArgs={'ServerSideEncryption': 'AES256'}
                )
            logger.info(f"Uploaded file to S3", path=local_path, key=s3_key)
            return True
        except Exception as e:
            logger.error(f"S3 upload failed", error=str(e))
            ERROR_COUNT.inc()
            return False

    async def download_file(self, s3_key: str, local_path: str) -> bool:
        """Download file from S3"""
        try:
            async with self.session.client('s3') as s3:
                await s3.download_file(
                    self.config.s3_bucket,
                    s3_key,
                    local_path
                )
            logger.info(f"Downloaded file from S3", key=s3_key, path=local_path)
            return True
        except Exception as e:
            logger.error(f"S3 download failed", error=str(e))
            ERROR_COUNT.inc()
            return False

class MessageQueueHandler:
    """Handles async message queue operations"""
    
    def __init__(self):
        self.session = aioboto3.Session()
        self.config = CloudConfig()

    async def send_message(self, message: Dict) -> bool:
        """Send message to SQS queue"""
        try:
            async with self.session.client('sqs') as sqs:
                await sqs.send_message(
                    QueueUrl=self.config.sqs_queue_url,
                    MessageBody=json.dumps(message)
                )
            return True
        except Exception as e:
            logger.error(f"Failed to send message", error=str(e))
            ERROR_COUNT.inc()
            return False

    async def receive_messages(self, max_messages: int = 10) -> List[Dict]:
        """Receive messages from SQS queue"""
        try:
            async with self.session.client('sqs') as sqs:
                response = await sqs.receive_message(
                    QueueUrl=self.config.sqs_queue_url,
                    MaxNumberOfMessages=max_messages
                )
                return response.get('Messages', [])
        except Exception as e:
            logger.error(f"Failed to receive messages", error=str(e))
            ERROR_COUNT.inc()
            return []

class AsyncDatabaseManager:
    """Manages async database operations"""
    
    def __init__(self):
        self.config = CloudConfig()
        self.pool = None

    async def connect(self):
        """Create connection pool"""
        self.pool = await aiomysql.create_pool(
            host=self.config.db_host,
            user=self.config.db_user,
            password=self.config.db_password,
            db=self.config.db_name,
            autocommit=False
        )

    async def close(self):
        """Close connection pool"""
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()

    async def execute_batch(self, query: str, params: List[tuple]):
        """Execute batch operation with transaction"""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                try:
                    await conn.begin()
                    await cur.executemany(query, params)
                    await conn.commit()
                except Exception as e:
                    await conn.rollback()
                    logger.error("Database operation failed", error=str(e))
                    ERROR_COUNT.inc()
                    raise

class AsyncETLPipeline:
    """Main ETL pipeline with async processing"""
    
    def __init__(self):
        self.storage = CloudStorageManager()
        self.queue = MessageQueueHandler()
        self.db = AsyncDatabaseManager()
        self.process_pool = ProcessPoolExecutor()

    async def process_file(self, file_path: str) -> Optional[pd.DataFrame]:
        """Process a single file asynchronously"""
        with PROCESSING_TIME.time():
            try:
                # Read file
                df = await asyncio.get_event_loop().run_in_executor(
                    self.process_pool,
                    partial(pd.read_csv, file_path)
                )
                
                # Validate and transform
                df = await self.validate_and_transform(df)
                
                # Update metrics
                PROCESSED_RECORDS.inc(len(df))
                
                return df
            except Exception as e:
                logger.error("File processing failed", file=file_path, error=str(e))
                ERROR_COUNT.inc()
                return None

    async def validate_and_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate and transform data"""
        # Run CPU-intensive operations in process pool
        return await asyncio.get_event_loop().run_in_executor(
            self.process_pool,
            self._transform_data,
            df
        )

    def _transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Synchronous data transformation"""
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

    async def load_to_database(self, df: pd.DataFrame):
        """Load data to database asynchronously"""
        if df.empty:
            return

        # Prepare data for insertion
        records = df.to_dict('records')
        query = """
            INSERT INTO users (name, age, email, processed_at)
            VALUES (%s, %s, %s, %s)
        """
        params = [
            (r['name'], r['age'], r['email'], r['processed_at'])
            for r in records
        ]
        
        await self.db.execute_batch(query, params)

    async def process_messages(self):
        """Process messages from queue"""
        while True:
            messages = await self.queue.receive_messages()
            if not messages:
                await asyncio.sleep(1)
                continue

            for message in messages:
                try:
                    data = json.loads(message['Body'])
                    s3_key = data['s3_key']
                    local_path = f"data/temp_{Path(s3_key).name}"

                    # Download file
                    if await self.storage.download_file(s3_key, local_path):
                        # Process file
                        df = await self.process_file(local_path)
                        if df is not None:
                            # Load to database
                            await self.load_to_database(df)
                            
                            # Upload processed file
                            processed_key = f"processed/{Path(s3_key).name}"
                            await self.storage.upload_file(local_path, processed_key)

                    # Cleanup
                    os.remove(local_path)
                except Exception as e:
                    logger.error("Message processing failed", error=str(e))
                    ERROR_COUNT.inc()

    async def run(self):
        """Run the ETL pipeline"""
        # Start metrics server
        start_http_server(8000)
        
        # Initialize database connection
        await self.db.connect()
        
        try:
            # Start message processing
            await self.process_messages()
        finally:
            # Cleanup
            await self.db.close()
            self.process_pool.shutdown()

async def main():
    """Main entry point"""
    pipeline = AsyncETLPipeline()
    await pipeline.run()

if __name__ == '__main__':
    asyncio.run(main())
