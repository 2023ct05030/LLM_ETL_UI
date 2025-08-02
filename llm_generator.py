import boto3
from typing import Dict, List, Optional, Tuple, Any
import os
import json
import pandas as pd
import numpy as np
from pandas.api.types import is_datetime64_any_dtype, is_numeric_dtype
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
    
    def _sanitize_for_json(self, data: Any) -> Any:
        """Sanitize data to ensure JSON compliance"""
        if isinstance(data, dict):
            return {k: self._sanitize_for_json(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._sanitize_for_json(item) for item in data]
        elif isinstance(data, (np.integer, np.int64, np.int32)):
            return int(data)
        elif isinstance(data, (np.floating, np.float64, np.float32)):
            if np.isnan(data) or np.isinf(data):
                return None
            return float(data)
        elif isinstance(data, np.bool_):
            return bool(data)
        elif isinstance(data, np.ndarray):
            return data.tolist()
        elif pd.isna(data):
            return None
        else:
            return data
    
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
        Generate ONLY clean, production-ready Python code with proper error handling, logging, and best practices.
        
        IMPORTANT: Return ONLY executable Python code. Do not include any explanatory text, markdown formatting, 
        or descriptions. Start directly with import statements or comments."""
        
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
        5. Use environment variables for credentials (they will be injected automatically)
        6. Implement data validation and quality checks
        7. Add progress tracking for large files
        8. Include table creation with appropriate data types
        9. Handle different file formats appropriately
        10. Add documentation and comments
        11. Use SNOWFLAKE_CONFIG dictionary for all Snowflake connections
        12. Use AWS_CONFIG dictionary for all AWS connections

        IMPORTANT CONFIGURATION NOTES:
        - DO NOT hardcode credentials in the script
        - Use SNOWFLAKE_CONFIG['account'], SNOWFLAKE_CONFIG['user'], etc.
        - Use AWS_CONFIG['aws_access_key_id'], AWS_CONFIG['region_name'], etc.
        - The configuration dictionaries will be automatically injected
        - Always check CONFIG_VALID before proceeding with operations

        STRUCTURE THE CODE WITH:
        - Imports and setup
        - Configuration validation using provided CONFIG_VALID
        - Helper functions
        - Main ETL class
        - Execution logic with proper error handling

        Please generate a complete, executable Python script that uses the injected configuration.
        """
        
        try:
            return self._invoke_bedrock_model(
                prompt=user_prompt,
                system_prompt=system_prompt,
                max_tokens=3000
            )
            
        except Exception as e:
            return f"Error generating code: {str(e)}"
    
    def generate_enhanced_etl_code(self, file_info: Dict, requirements: str, profiling_data: Dict = None) -> str:
        """Generate enhanced ETL code with data profiling insights"""
        
        file_extension = file_info.get('original_filename', '').split('.')[-1].lower()
        
        system_prompt = """You are an expert Python developer specializing in ETL processes with AWS S3 and Snowflake. 
        Generate ONLY clean, production-ready Python code with proper error handling, logging, and best practices.
        Use the provided data profiling insights to optimize the ETL process.
        
        IMPORTANT: Return ONLY executable Python code. Do not include any explanatory text, markdown formatting, 
        or descriptions. Start directly with import statements or comments."""
        
        # Base prompt
        user_prompt = f"""
        Generate a complete Python ETL script with the following requirements:

        FILE INFORMATION:
        - S3 URL: {file_info.get('s3_url', 'N/A')}
        - Original filename: {file_info.get('original_filename', 'N/A')}
        - File type: {file_extension}
        - Content type: {file_info.get('content_type', 'N/A')}

        USER REQUIREMENTS:
        {requirements}
        """
        
        # Add profiling insights if available
        if profiling_data and profiling_data.get('success'):
            user_prompt += f"""

        DATA PROFILING INSIGHTS:
        """
            
            # Dataset information
            if 'dataset_info' in profiling_data:
                dataset = profiling_data['dataset_info']
                user_prompt += f"""
        - Dataset: {dataset.get('rows', 'N/A')} rows × {dataset.get('columns', 'N/A')} columns
        - Columns: {', '.join(dataset.get('column_names', []))}
        - Data types: {dataset.get('dtypes', {})}
        """
            
            # Primary key candidates
            if profiling_data.get('primary_key_candidates'):
                pk_candidates = [pk.get('column', pk) if isinstance(pk, dict) else pk 
                               for pk in profiling_data['primary_key_candidates']]
                user_prompt += f"""
        - Primary Key Candidates: {', '.join(pk_candidates)}
          Use these for deduplication and as table primary keys.
        """
            
            # Date columns
            if profiling_data.get('date_columns'):
                date_cols = [dc.get('column', dc) if isinstance(dc, dict) else dc 
                           for dc in profiling_data['date_columns']]
                user_prompt += f"""
        - Date/Time Columns: {', '.join(date_cols)}
          Implement proper date parsing and consider SCD2 if appropriate.
        """
            
            # Data quality insights
            if 'data_quality' in profiling_data:
                quality = profiling_data['data_quality']
                if 'summary' in quality:
                    user_prompt += f"""
        - Data Quality: {quality['summary'].get('overall_completeness', 'N/A')}% complete
        - Data Size: {quality['summary'].get('data_size_mb', 'N/A')} MB
        """
                
                # Add column-specific quality issues
                if 'completeness' in quality:
                    poor_quality_cols = [col for col, info in quality['completeness'].items() 
                                       if info.get('status') == 'poor']
                    if poor_quality_cols:
                        user_prompt += f"""
        - Columns with poor data quality (>20% nulls): {', '.join(poor_quality_cols)}
          Implement special handling for these columns.
        """
            
            # Schema recommendations
            if 'schema_recommendations' in profiling_data:
                schema = profiling_data['schema_recommendations']
                if 'columns' in schema:
                    user_prompt += f"""
        - Recommended Snowflake Schema:
        """
                    for col in schema['columns'][:5]:  # Show first 5 columns
                        user_prompt += f"  • {col.get('name', '')}: {col.get('type', '')} {'NOT NULL' if not col.get('nullable', True) else ''}"
            
            # LLM insights
            if profiling_data.get('llm_insights'):
                user_prompt += f"""
        
        EXPERT ANALYSIS:
        {profiling_data['llm_insights'][:500]}...  # Truncate for token limit
        """
        
        user_prompt += f"""

        ENHANCED TECHNICAL REQUIREMENTS:
        1. Use boto3 for S3 operations
        2. Use snowflake-connector-python for Snowflake operations  
        3. Use pandas for data manipulation
        4. Implement data profiling insights in table design
        5. Add data quality validations based on profiling results
        6. Use recommended data types from profiling
        7. Handle identified primary keys appropriately
        8. Implement proper date/time parsing for identified columns
        9. Add progress tracking for large files
        10. Include comprehensive error handling and logging
        11. Add data quality monitoring and alerting
        12. Optimize for the identified data patterns
        13. Use SNOWFLAKE_CONFIG dictionary for all Snowflake connections
        14. Use AWS_CONFIG dictionary for all AWS connections
        15. Always validate CONFIG_VALID before proceeding

        IMPORTANT CONFIGURATION NOTES:
        - DO NOT hardcode credentials in the script
        - Use SNOWFLAKE_CONFIG and AWS_CONFIG dictionaries
        - Configuration will be automatically injected by the workflow
        - Check CONFIG_VALID flag before executing operations
        - Implement graceful fallback for missing configuration

        STRUCTURE THE CODE WITH:
        - Imports and setup
        - Configuration validation using provided CONFIG_VALID  
        - Data profiling utilities
        - Enhanced ETL class with profiling integration
        - Optimized table creation using profiling insights
        - Data quality validation functions
        - Main execution logic
        - Comprehensive error handling

        Generate a complete, production-ready Python script that leverages all profiling insights and uses the injected configuration properly.
        """
        
        try:
            return self._invoke_bedrock_model(
                prompt=user_prompt,
                system_prompt=system_prompt,
                max_tokens=4000  # Increased for enhanced code
            )
            
        except Exception as e:
            return f"Error generating enhanced code: {str(e)}"
    
    def generate_profiling_summary(self, profiling_data: Dict) -> str:
        """Generate a human-readable summary of profiling results"""
        if not profiling_data.get('success'):
            return f"❌ Data profiling failed: {profiling_data.get('error', 'Unknown error')}"
        
        summary_parts = []
        
        # Dataset overview
        if 'dataset_info' in profiling_data:
            dataset = profiling_data['dataset_info']
            summary_parts.append(
                f"📊 **Dataset Overview:** {dataset.get('rows', 'N/A')} rows × {dataset.get('columns', 'N/A')} columns"
            )
        
        # Data quality
        if 'data_quality' in profiling_data and 'summary' in profiling_data['data_quality']:
            quality = profiling_data['data_quality']['summary']
            completeness = quality.get('overall_completeness', 0)
            quality_icon = "🟢" if completeness >= 95 else "🟡" if completeness >= 80 else "🔴"
            summary_parts.append(
                f"{quality_icon} **Data Quality:** {completeness:.1f}% complete ({quality.get('data_size_mb', 0):.2f} MB)"
            )
        
        # Primary keys
        if profiling_data.get('primary_key_candidates'):
            pk_candidates = []
            for pk in profiling_data['primary_key_candidates']:
                if isinstance(pk, dict):
                    confidence_icon = "🔑" if pk.get('confidence') == 'high' else "🗝️"
                    pk_candidates.append(f"{confidence_icon} {pk.get('column', 'N/A')}")
                else:
                    pk_candidates.append(f"🔑 {pk}")
            
            if pk_candidates:
                summary_parts.append(f"**Primary Key Candidates:** {', '.join(pk_candidates)}")
        
        # Date columns
        if profiling_data.get('date_columns'):
            date_cols = []
            for dc in profiling_data['date_columns']:
                if isinstance(dc, dict):
                    confidence_icon = "📅" if dc.get('confidence') == 'high' else "📆"
                    date_cols.append(f"{confidence_icon} {dc.get('column', 'N/A')}")
                else:
                    date_cols.append(f"📅 {dc}")
            
            if date_cols:
                summary_parts.append(f"**Date/Time Columns:** {', '.join(date_cols)}")
        
        # Data quality issues
        if 'data_quality' in profiling_data and 'completeness' in profiling_data['data_quality']:
            poor_quality_cols = []
            warning_cols = []
            
            for col, info in profiling_data['data_quality']['completeness'].items():
                if info.get('status') == 'poor':
                    poor_quality_cols.append(f"🔴 {col} ({info.get('null_percentage', 0):.1f}% nulls)")
                elif info.get('status') == 'warning':
                    warning_cols.append(f"🟡 {col} ({info.get('null_percentage', 0):.1f}% nulls)")
            
            if poor_quality_cols:
                summary_parts.append(f"**Data Quality Issues:** {', '.join(poor_quality_cols)}")
            elif warning_cols:
                summary_parts.append(f"**Data Quality Warnings:** {', '.join(warning_cols)}")
        
        # Schema recommendations preview
        if 'schema_recommendations' in profiling_data and 'columns' in profiling_data['schema_recommendations']:
            col_count = len(profiling_data['schema_recommendations']['columns'])
            summary_parts.append(f"✅ **Schema Ready:** {col_count} columns mapped to Snowflake types")
        
        return "\n".join(summary_parts)
    
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
    
    def profile_data_from_s3(self, s3_url: str, bucket_name: str = None) -> Dict[str, Any]:
        """Comprehensive data profiling for uploaded files using Nova model"""
        try:
            # Parse S3 URL
            if s3_url.startswith('s3://'):
                parts = s3_url[5:].split('/', 1)
                bucket = bucket_name or parts[0]
                key = parts[1]
            else:
                raise ValueError("Invalid S3 URL format")
            
            # Load data from S3
            df = self._load_csv_from_s3(bucket, key)
            
            if df is None:
                return {"error": "Failed to load data from S3"}
            
            # Perform data analysis
            primary_key_candidates = self._find_primary_key_candidates(df)
            date_columns = self._find_date_columns(df)
            data_quality_report = self._analyze_data_quality(df)
            schema_recommendations = self._generate_schema_recommendations(df)
            
            # Generate LLM-powered insights
            llm_analysis = self._generate_llm_data_insights(
                df, primary_key_candidates, date_columns, data_quality_report
            )
            
            result = {
                "success": True,
                "dataset_info": {
                    "rows": len(df),
                    "columns": len(df.columns),
                    "column_names": list(df.columns),
                    "dtypes": df.dtypes.astype(str).to_dict()
                },
                "primary_key_candidates": primary_key_candidates,
                "date_columns": date_columns,
                "data_quality": data_quality_report,
                "schema_recommendations": schema_recommendations,
                "llm_insights": llm_analysis,
                "sample_data": df.head(5).to_dict('records') if len(df) > 0 else []
            }
            
            # Sanitize for JSON compliance
            return self._sanitize_for_json(result)
            
        except Exception as e:
            return {
                "success": False,
                "error": f"Data profiling failed: {str(e)}"
            }
    
    def _load_csv_from_s3(self, bucket: str, key: str) -> Optional[pd.DataFrame]:
        """Load CSV data from S3"""
        try:
            s3_client = boto3.client(
                's3',
                region_name=Config.AWS_REGION,
                aws_access_key_id=Config.AWS_ACCESS_KEY_ID,
                aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY
            )
            
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            df = pd.read_csv(obj['Body'])
            return df
            
        except Exception as e:
            print(f"Error loading CSV from S3: {str(e)}")
            return None
    
    def _find_primary_key_candidates(self, df: pd.DataFrame) -> List[str]:
        """Identify potential primary key columns"""
        candidates = []
        n = len(df)
        
        for col in df.columns:
            unique_count = df[col].nunique(dropna=True)
            null_count = df[col].isnull().sum()
            
            # Perfect primary key: unique for all rows, no nulls
            if unique_count == n and null_count == 0:
                candidates.append({
                    "column": col,
                    "confidence": "high",
                    "reason": "100% unique, no nulls"
                })
            # Good candidate: mostly unique, few nulls
            elif unique_count >= n * 0.95 and null_count <= n * 0.05:
                candidates.append({
                    "column": col,
                    "confidence": "medium",
                    "reason": f"{unique_count/n*100:.1f}% unique, {null_count/n*100:.1f}% nulls"
                })
        
        return candidates
    
    def _find_date_columns(self, df: pd.DataFrame) -> List[str]:
        """Identify date/time columns for SCD2 operations"""
        candidates = []
        
        for col in df.columns:
            date_info = {"column": col, "confidence": "low", "reason": ""}
            
            # Check if already datetime
            if is_datetime64_any_dtype(df[col]):
                if df[col].nunique(dropna=True) > 1:
                    date_info.update({
                        "confidence": "high",
                        "reason": "Already datetime type with multiple values"
                    })
                    candidates.append(date_info)
                continue
            
            # Skip numeric columns
            if is_numeric_dtype(df[col]):
                continue
            
            # Try parsing as datetime
            try:
                parsed = pd.to_datetime(df[col], errors='coerce')
                valid_dates = parsed.notna().sum()
                unique_dates = parsed.nunique(dropna=True)
                
                if valid_dates >= len(df) * 0.8 and unique_dates > 1:
                    date_info.update({
                        "confidence": "high",
                        "reason": f"{valid_dates/len(df)*100:.1f}% valid dates, {unique_dates} unique values"
                    })
                    candidates.append(date_info)
                elif valid_dates >= len(df) * 0.5:
                    date_info.update({
                        "confidence": "medium",
                        "reason": f"{valid_dates/len(df)*100:.1f}% valid dates"
                    })
                    candidates.append(date_info)
            except:
                pass
            
            # Check column name patterns
            if any(pattern in col.lower() for pattern in ['date', 'time', 'created', 'updated', 'modified', '_dt', '_ts']):
                if date_info not in candidates:
                    date_info.update({
                        "confidence": "medium",
                        "reason": "Column name suggests date/time"
                    })
                    candidates.append(date_info)
        
        return candidates
    
    def _analyze_data_quality(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze data quality metrics"""
        quality_report = {
            "completeness": {},
            "uniqueness": {},
            "data_types": {},
            "outliers": {},
            "summary": {}
        }
        
        for col in df.columns:
            # Completeness
            null_count = df[col].isnull().sum()
            null_percentage = (null_count / len(df)) * 100 if len(df) > 0 else 0
            quality_report["completeness"][col] = {
                "null_count": int(null_count),
                "null_percentage": round(float(null_percentage), 2),
                "status": "good" if null_percentage < 5 else "warning" if null_percentage < 20 else "poor"
            }
            
            # Uniqueness
            unique_count = df[col].nunique(dropna=True)
            uniqueness_percentage = (unique_count / len(df)) * 100 if len(df) > 0 else 0
            quality_report["uniqueness"][col] = {
                "unique_count": int(unique_count),
                "uniqueness_percentage": round(float(uniqueness_percentage), 2)
            }
            
            # Data type analysis
            quality_report["data_types"][col] = {
                "current_type": str(df[col].dtype),
                "suggested_type": self._suggest_data_type(df[col])
            }
        
        # Overall summary
        avg_completeness = sum(100 - v["null_percentage"] for v in quality_report["completeness"].values()) / len(df.columns) if len(df.columns) > 0 else 0
        memory_usage = df.memory_usage(deep=True).sum() / 1024 / 1024
        
        quality_report["summary"] = {
            "overall_completeness": round(float(avg_completeness), 2),
            "total_rows": int(len(df)),
            "total_columns": int(len(df.columns)),
            "data_size_mb": round(float(memory_usage), 2)
        }
        
        return quality_report
    
    def _suggest_data_type(self, series: pd.Series) -> str:
        """Suggest optimal data type for a column"""
        if series.dtype == 'object':
            # Try to infer better type
            if series.dropna().empty:
                return "varchar"
            
            # Check if it's numeric
            try:
                pd.to_numeric(series.dropna())
                return "numeric"
            except:
                pass
            
            # Check if it's date
            try:
                pd.to_datetime(series.dropna())
                return "timestamp"
            except:
                pass
            
            # Check string length for varchar sizing
            max_length = series.astype(str).str.len().max()
            if pd.isna(max_length):
                max_length = 255
            else:
                max_length = int(max_length)
            
            if max_length <= 50:
                return f"varchar({max_length})"
            elif max_length <= 255:
                return f"varchar({max_length})"
            else:
                return "text"
        
        return str(series.dtype)
    
    def _generate_schema_recommendations(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Generate Snowflake schema recommendations"""
        schema = {
            "table_name": "extracted_data",
            "columns": [],
            "constraints": [],
            "indexes": []
        }
        
        for col in df.columns:
            col_info = {
                "name": col.lower().replace(' ', '_').replace('-', '_'),
                "original_name": col,
                "type": self._suggest_snowflake_type(df[col]),
                "nullable": df[col].isnull().any(),
                "unique": df[col].nunique() == len(df)
            }
            schema["columns"].append(col_info)
        
        return schema
    
    def _suggest_snowflake_type(self, series: pd.Series) -> str:
        """Suggest Snowflake data type"""
        if is_numeric_dtype(series):
            if series.dtype in ['int64', 'int32']:
                return "NUMBER(38,0)"
            else:
                return "NUMBER(38,2)"
        elif is_datetime64_any_dtype(series):
            return "TIMESTAMP_NTZ"
        else:
            max_length = series.astype(str).str.len().max() if not series.empty else 255
            if pd.isna(max_length):
                max_length = 255
            else:
                max_length = int(max_length)
            
            if max_length <= 255:
                return f"VARCHAR({max_length})"
            else:
                return "TEXT"
    
    def _generate_llm_data_insights(self, df: pd.DataFrame, primary_keys: List[str], 
                                   date_columns: List[str], quality_report: Dict) -> str:
        """Generate LLM-powered insights about the data"""
        
        # Prepare summary for LLM
        summary_stats = df.describe(include='all').to_string() if len(df) > 0 else "No data available"
        sample_data = df.head(3).to_string() if len(df) > 0 else "No data available"
        
        prompt = f"""
        Analyze this dataset and provide comprehensive data profiling insights:

        DATASET OVERVIEW:
        - Rows: {len(df)}
        - Columns: {len(df.columns)}
        - Column Names: {', '.join(df.columns)}

        SAMPLE DATA (first 3 rows):
        {sample_data}

        STATISTICAL SUMMARY:
        {summary_stats}

        IDENTIFIED PATTERNS:
        - Primary Key Candidates: {[pk['column'] for pk in primary_keys]}
        - Date/Time Columns: {[dc['column'] for dc in date_columns]}
        - Data Quality Score: {quality_report.get('summary', {}).get('overall_completeness', 0):.1f}%

        Please provide insights on:
        1. Data quality assessment and recommendations
        2. Suggested ETL transformations
        3. Snowflake schema optimization recommendations
        4. Potential data issues or anomalies
        5. SCD2 implementation strategy (if applicable)
        6. Performance optimization suggestions
        7. Data governance considerations

        Keep the analysis practical and actionable for ETL development.
        """
        
        try:
            return self._invoke_bedrock_model(
                prompt=prompt,
                system_prompt="You are a senior data engineer providing expert data profiling analysis. Focus on practical ETL and Snowflake optimization recommendations.",
                max_tokens=1500
            )
        except Exception as e:
            return f"Error generating LLM insights: {str(e)}"