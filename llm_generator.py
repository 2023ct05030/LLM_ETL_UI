import boto3
from typing import Dict, List, Optional
import os
import json
from config import Config

class LLMCodeGenerator:
    """LLM-powered code generator for ETL processes using AWS Bedrock Nova Micro"""
    
    def __init__(self):
        self.bedrock_client = boto3.client(
            'bedrock-runtime',
            region_name=Config.BEDROCK_REGION,
            aws_access_key_id=Config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY
        )
        self.model_id = Config.NOVA_MODEL_ID
    
    def _invoke_bedrock_model(self, prompt: str, system_prompt: str = None, max_tokens: int = 3000) -> str:
        """Invoke AWS Bedrock Nova Micro model"""
        try:
            # Prepare the request body for Nova Micro
            request_body = {
                "messages": [
                    {
                        "role": "user",
                        "content": [{"text": f"{system_prompt}\n\n{prompt}" if system_prompt else prompt}]
                    }
                ],
                "inferenceConfig": {
                    "maxTokens": max_tokens,
                    "temperature": 0.1,
                    "topP": 0.9
                }
            }
            
            response = self.bedrock_client.invoke_model(
                modelId=self.model_id,
                body=json.dumps(request_body),
                contentType='application/json'
            )
            
            response_body = json.loads(response['body'].read())
            return response_body['output']['message']['content'][0]['text']
            
        except Exception as e:
            return f"Error invoking Bedrock model: {str(e)}"
    
    def generate_etl_code(self, file_info: Dict, requirements: str) -> str:
        """Generate ETL code based on file info and requirements"""
        
        file_extension = file_info.get('original_filename', '').split('.')[-1].lower()
        
        system_prompt = """You are an expert Python developer specializing in ETL processes with AWS S3 and Snowflake. 
        Generate clean, production-ready Python code with proper error handling, logging, and best practices."""
        
        user_prompt = f"""
        Generate a complete Python ETL script with the following requirements:

        FILE INFORMATION:
        - S3 URL: {file_info.get('s3_url', 'N/A')}
        - Original filename: {file_info.get('original_filename', 'N/A')}
        - File type: {file_extension}
        - Content type: {file_info.get('content_type', 'N/A')}

        USER REQUIREMENTS:
        {requirements}

        TECHNICAL REQUIREMENTS:
        1. Use boto3 for S3 operations
        2. Use snowflake-connector-python for Snowflake operations
        3. Use pandas for data manipulation
        4. Include proper error handling and logging
        5. Use environment variables for credentials
        6. Implement data validation and quality checks
        7. Add progress tracking for large files
        8. Include table creation with appropriate data types
        9. Handle different file formats appropriately
        10. Add documentation and comments

        STRUCTURE THE CODE WITH:
        - Imports and setup
        - Configuration and logging
        - Helper functions
        - Main ETL class
        - Execution logic
        - Error handling

        Please generate a complete, executable Python script.
        """
        
        try:
            return self._invoke_bedrock_model(
                prompt=user_prompt,
                system_prompt=system_prompt,
                max_tokens=3000
            )
            
        except Exception as e:
            return f"Error generating code: {str(e)}"
    
    def generate_data_analysis(self, file_info: Dict) -> str:
        """Generate data analysis recommendations"""
        
        prompt = f"""
        Based on the uploaded file information, provide recommendations for:
        
        File: {file_info.get('original_filename', 'N/A')}
        Type: {file_info.get('content_type', 'N/A')}
        
        Please suggest:
        1. Appropriate data validation checks
        2. Recommended data transformations
        3. Snowflake table design best practices
        4. Performance optimization strategies
        5. Data quality monitoring approaches
        
        Keep recommendations practical and actionable.
        """
        
        try:
            return self._invoke_bedrock_model(
                prompt=prompt,
                system_prompt="You are a data engineering expert providing practical advice.",
                max_tokens=1000
            )
            
        except Exception as e:
            return f"Error generating analysis: {str(e)}"
    
    def explain_etl_process(self, file_type: str) -> str:
        """Explain ETL process for specific file type"""
        
        prompt = f"""
        Explain the ETL process for ingesting {file_type} files into Snowflake, including:
        
        1. Data extraction best practices
        2. Common transformation requirements
        3. Loading strategies and considerations
        4. Potential challenges and solutions
        5. Performance optimization tips
        
        Keep the explanation clear and technical.
        """
        
        try:
            return self._invoke_bedrock_model(
                prompt=prompt,
                system_prompt="You are an ETL expert explaining technical processes clearly.",
                max_tokens=800
            )
            
        except Exception as e:
            return f"Error generating explanation: {str(e)}"
