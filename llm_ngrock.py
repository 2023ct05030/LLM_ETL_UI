import requests
from typing import Dict, List, Optional
import os
import json
from config import Config

class LLMCodeGenerator:
    """LLM-powered code generator for ETL processes using external API via ngrok"""
    
    def __init__(self):
        self.ngrok_url = Config.NGROK_URL
        if not self.ngrok_url:
            raise ValueError("NGROK_URL must be configured in config.py or environment variables")
    
    def _invoke_llm_model(self, prompt: str, system_prompt: str = None, max_tokens: int = 3000) -> str:
        """Invoke LLM model via ngrok API"""
        try:
            # Combine system prompt and user prompt if system prompt exists
            full_prompt = f"{system_prompt}\n\n{prompt}" if system_prompt else prompt
            
            response = requests.post(
                f"{self.ngrok_url}/predict",
                json={"prompt": full_prompt},
                timeout=60
            )
            response.raise_for_status()
            result = response.json()
            return result.get("response", "").strip()
            
        except requests.exceptions.RequestException as e:
            return f"Error during API call: {e}"
        except Exception as e:
            return f"Error invoking LLM model: {str(e)}"
    
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

        Please generate a complete, executable Python script.
        """
        
        try:
            return self._invoke_llm_model(
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
            return self._invoke_llm_model(
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
            return self._invoke_llm_model(
                prompt=prompt,
                system_prompt="You are an ETL expert explaining technical processes clearly.",
                max_tokens=800
            )
            
        except Exception as e:
            return f"Error generating explanation: {str(e)}"
