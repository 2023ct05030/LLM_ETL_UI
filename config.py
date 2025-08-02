import os
from typing import Optional
import logging

class Config:
    """Application configuration"""
    
    # AWS Configuration
    AWS_ACCESS_KEY_ID: Optional[str] = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY: Optional[str] = os.getenv('AWS_SECRET_ACCESS_KEY')
    AWS_REGION: str = os.getenv('AWS_REGION', 'us-east-1')
    S3_BUCKET_NAME: Optional[str] = os.getenv('S3_BUCKET_NAME')
    
    # AWS Bedrock Configuration
    BEDROCK_REGION: str = os.getenv('BEDROCK_REGION', AWS_REGION)
    NOVA_MODEL_ID: str = os.getenv('NOVA_MODEL_ID', 'amazon.nova-micro-v1:0')
    
    # Ngrok Configuration for LLM API
    NGROK_URL: Optional[str] = os.getenv('NGROK_URL', 'https://9ba3d7e4331c.ngrok-free.app')
    
    # Snowflake Configuration
    SNOWFLAKE_ACCOUNT: Optional[str] = os.getenv('SNOWFLAKE_ACCOUNT')
    SNOWFLAKE_USER: Optional[str] = os.getenv('SNOWFLAKE_USER')
    SNOWFLAKE_PASSWORD: Optional[str] = os.getenv('SNOWFLAKE_PASSWORD')
    SNOWFLAKE_WAREHOUSE: Optional[str] = os.getenv('SNOWFLAKE_WAREHOUSE')
    SNOWFLAKE_DATABASE: Optional[str] = os.getenv('SNOWFLAKE_DATABASE')
    SNOWFLAKE_SCHEMA: Optional[str] = os.getenv('SNOWFLAKE_SCHEMA')
    
    # Application Settings
    SECRET_KEY: str = os.getenv('SECRET_KEY', 'your-secret-key-change-in-production')
    ENVIRONMENT: str = os.getenv('ENVIRONMENT', 'development')
    DEBUG: bool = ENVIRONMENT == 'development'
    
    # File Upload Settings
    MAX_FILE_SIZE: int = int(os.getenv('MAX_FILE_SIZE', '100')) * 1024 * 1024  # 100MB default
    ALLOWED_EXTENSIONS: list = ['.csv', '.json', '.xlsx', '.xls', '.txt', '.parquet']
    
    # Logging Configuration
    LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO')
    
    @classmethod
    def validate_config(cls) -> bool:
        """Validate required configuration"""
        required_fields = [
            'AWS_ACCESS_KEY_ID',
            'AWS_SECRET_ACCESS_KEY',
            'S3_BUCKET_NAME',
            'SNOWFLAKE_ACCOUNT',
            'SNOWFLAKE_USER',
            'SNOWFLAKE_PASSWORD'
        ]
        
        missing_fields = []
        for field in required_fields:
            if not getattr(cls, field):
                missing_fields.append(field)
        
        if missing_fields:
            logging.error(f"Missing required configuration: {', '.join(missing_fields)}")
            return False
        
        return True

# Configure logging
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
