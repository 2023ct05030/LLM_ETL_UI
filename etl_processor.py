import os
import boto3
import pandas as pd
import snowflake.connector
from typing import Dict, Any, Optional
import logging
from datetime import datetime
import json

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ETLProcessor:
    """ETL processor for handling file uploads and Snowflake ingestion"""
    
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION', 'us-east-1')
        )
        
        self.snowflake_config = {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA')
        }
    
    def read_file_from_s3(self, s3_url: str) -> pd.DataFrame:
        """Read file from S3 and return as DataFrame"""
        try:
            # Parse S3 URL
            parts = s3_url.replace('s3://', '').split('/')
            bucket = parts[0]
            key = '/'.join(parts[1:])
            
            # Download file
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            
            # Determine file type and read accordingly
            if key.endswith('.csv'):
                df = pd.read_csv(response['Body'])
            elif key.endswith('.json'):
                df = pd.read_json(response['Body'])
            elif key.endswith('.xlsx') or key.endswith('.xls'):
                df = pd.read_excel(response['Body'])
            elif key.endswith('.parquet'):
                df = pd.read_parquet(response['Body'])
            else:
                raise ValueError(f"Unsupported file type: {key}")
            
            logger.info(f"Successfully read file from S3: {s3_url}")
            return df
            
        except Exception as e:
            logger.error(f"Error reading file from S3: {str(e)}")
            raise
    
    def infer_snowflake_schema(self, df: pd.DataFrame) -> Dict[str, str]:
        """Infer Snowflake schema from DataFrame"""
        schema = {}
        
        for column in df.columns:
            dtype = df[column].dtype
            
            if dtype == 'object':
                # Check if it's a date string
                try:
                    pd.to_datetime(df[column].dropna().iloc[0])
                    schema[column] = 'TIMESTAMP'
                except:
                    # Determine string length
                    max_length = df[column].astype(str).str.len().max()
                    if max_length <= 255:
                        schema[column] = f'VARCHAR({max_length})'
                    else:
                        schema[column] = 'TEXT'
            elif dtype in ['int64', 'int32']:
                schema[column] = 'INTEGER'
            elif dtype in ['float64', 'float32']:
                schema[column] = 'FLOAT'
            elif dtype == 'bool':
                schema[column] = 'BOOLEAN'
            elif dtype == 'datetime64[ns]':
                schema[column] = 'TIMESTAMP'
            else:
                schema[column] = 'VARCHAR(255)'
        
        return schema
    
    def create_snowflake_table(self, table_name: str, schema: Dict[str, str]) -> str:
        """Create Snowflake table with given schema"""
        try:
            conn = snowflake.connector.connect(**self.snowflake_config)
            cursor = conn.cursor()
            
            # Create table SQL
            columns = [f"{col} {dtype}" for col, dtype in schema.items()]
            create_sql = f"""
            CREATE OR REPLACE TABLE {table_name} (
                {', '.join(columns)},
                etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
            )
            """
            
            cursor.execute(create_sql)
            conn.commit()
            
            logger.info(f"Successfully created table: {table_name}")
            return create_sql
            
        except Exception as e:
            logger.error(f"Error creating Snowflake table: {str(e)}")
            raise
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()
    
    def load_data_to_snowflake(self, df: pd.DataFrame, table_name: str) -> int:
        """Load DataFrame to Snowflake table"""
        try:
            conn = snowflake.connector.connect(**self.snowflake_config)
            
            # Write DataFrame to Snowflake
            from snowflake.connector.pandas_tools import write_pandas
            
            success, nchunks, nrows, _ = write_pandas(
                conn, 
                df, 
                table_name, 
                auto_create_table=False,
                overwrite=True
            )
            
            if success:
                logger.info(f"Successfully loaded {nrows} rows to {table_name}")
                return nrows
            else:
                raise Exception("Failed to load data to Snowflake")
                
        except Exception as e:
            logger.error(f"Error loading data to Snowflake: {str(e)}")
            raise
        finally:
            if 'conn' in locals():
                conn.close()
    
    def execute_etl_pipeline(self, s3_url: str, table_name: str) -> Dict[str, Any]:
        """Execute complete ETL pipeline"""
        try:
            start_time = datetime.now()
            
            # Step 1: Read data from S3
            logger.info("Step 1: Reading data from S3...")
            df = self.read_file_from_s3(s3_url)
            
            # Step 2: Infer schema
            logger.info("Step 2: Inferring schema...")
            schema = self.infer_snowflake_schema(df)
            
            # Step 3: Create table
            logger.info("Step 3: Creating Snowflake table...")
            create_sql = self.create_snowflake_table(table_name, schema)
            
            # Step 4: Load data
            logger.info("Step 4: Loading data to Snowflake...")
            rows_loaded = self.load_data_to_snowflake(df, table_name)
            
            end_time = datetime.now()
            
            return {
                "status": "success",
                "rows_loaded": rows_loaded,
                "table_name": table_name,
                "schema": schema,
                "create_sql": create_sql,
                "execution_time": str(end_time - start_time),
                "timestamp": end_time.isoformat()
            }
            
        except Exception as e:
            logger.error(f"ETL pipeline failed: {str(e)}")
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
