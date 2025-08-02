from fastapi import FastAPI, UploadFile, File, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
import boto3
import os
import uuid
import json
from datetime import datetime
from typing import Optional, Dict, Any
from pydantic import BaseModel
import aiofiles
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector

# Load environment variables BEFORE importing config
load_dotenv()

from config import Config
from llm_generator import LLMCodeGenerator
from langgraph_etl_workflow import run_etl_workflow

# Validate configuration
if not Config.validate_config():
    print("Warning: Some required configuration is missing. Please check your .env file.")

app = FastAPI(
    title="LLM ETL Frontend API",
    description="API for LLM-powered ETL code generation with file upload to S3 and Snowflake integration",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:8000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models
class ChatMessage(BaseModel):
    message: str
    file_url: Optional[str] = None
    file_name: Optional[str] = None

class ChatResponse(BaseModel):
    response: str
    etl_code: Optional[str] = None
    execution_status: Optional[str] = None

class ETLWorkflowRequest(BaseModel):
    file_url: str
    file_name: str
    requirements: str
    auto_execute: Optional[bool] = True

class ETLWorkflowResponse(BaseModel):
    success: bool
    workflow_id: Optional[str] = None
    script_path: Optional[str] = None
    execution_success: Optional[bool] = None
    snowflake_success: Optional[bool] = None
    records_inserted: Optional[int] = None
    execution_output: Optional[str] = None
    errors: Optional[Dict[str, str]] = None
    summary: Optional[str] = None
    timestamp: str

# AWS S3 client
try:
    s3_client = boto3.client(
        's3',
        aws_access_key_id=Config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY,
        region_name=Config.AWS_REGION
    )
    print(f"S3 client initialized for region: {Config.AWS_REGION}")
except Exception as e:
    print(f"Failed to initialize S3 client: {str(e)}")
    s3_client = None

# Initialize LLM code generator
llm_generator = LLMCodeGenerator()

# Snowflake connection parameters
SNOWFLAKE_CONFIG = {
    'account': Config.SNOWFLAKE_ACCOUNT,
    'user': Config.SNOWFLAKE_USER,
    'password': Config.SNOWFLAKE_PASSWORD,
    'warehouse': Config.SNOWFLAKE_WAREHOUSE,
    'database': Config.SNOWFLAKE_DATABASE,
    'schema': Config.SNOWFLAKE_SCHEMA
}

def get_snowflake_connection():
    """Create Snowflake connection"""
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Snowflake connection failed: {str(e)}")

async def upload_to_s3(file: UploadFile) -> Dict[str, str]:
    """Upload file to S3 and return file info"""
    try:
        # Check if S3 client is available
        if s3_client is None:
            raise HTTPException(status_code=500, detail="S3 client not initialized. Check AWS credentials.")
        
        # Validate configuration
        if not Config.S3_BUCKET_NAME:
            raise HTTPException(status_code=500, detail="S3 bucket name not configured")
        
        if not Config.AWS_ACCESS_KEY_ID or not Config.AWS_SECRET_ACCESS_KEY:
            raise HTTPException(status_code=500, detail="AWS credentials not configured")
        
        # Generate unique filename
        file_extension = file.filename.split('.')[-1] if '.' in file.filename else ''
        unique_filename = f"{uuid.uuid4()}.{file_extension}"
        
        # Read file content
        file_content = await file.read()
        
        # Validate file size
        if len(file_content) > Config.MAX_FILE_SIZE:
            raise HTTPException(
                status_code=413, 
                detail=f"File too large. Maximum size: {Config.MAX_FILE_SIZE / (1024*1024):.1f}MB"
            )
        
        # Validate file extension
        if f".{file_extension.lower()}" not in Config.ALLOWED_EXTENSIONS:
            raise HTTPException(
                status_code=400, 
                detail=f"File type not supported. Allowed: {', '.join(Config.ALLOWED_EXTENSIONS)}"
            )
        
        # Upload to S3
        s3_client.put_object(
            Bucket=Config.S3_BUCKET_NAME,
            Key=unique_filename,
            Body=file_content,
            ContentType=file.content_type or 'application/octet-stream'
        )
        
        # Generate S3 URL
        s3_url = f"s3://{Config.S3_BUCKET_NAME}/{unique_filename}"
        
        return {
            "s3_url": s3_url,
            "filename": unique_filename,
            "original_filename": file.filename,
            "content_type": file.content_type or 'application/octet-stream',
            "size": len(file_content)
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"S3 upload error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"S3 upload failed: {str(e)}")

def generate_etl_code(file_info: Dict[str, str], user_requirements: str) -> str:
    """Generate ETL code using AWS Bedrock Nova Micro"""
    try:
        return llm_generator.generate_etl_code(file_info, user_requirements)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"ETL code generation failed: {str(e)}")

@app.get("/", response_class=HTMLResponse)
async def read_root():
    """Serve the main HTML page"""
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>LLM ETL Chat Application</title>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <style>
            body { 
                font-family: Arial, sans-serif; 
                margin: 0; 
                padding: 20px; 
                background: #f5f5f5; 
            }
            .container { 
                max-width: 1200px; 
                margin: 0 auto; 
                background: white; 
                border-radius: 8px; 
                box-shadow: 0 2px 10px rgba(0,0,0,0.1); 
            }
            .header { 
                background: #2563eb; 
                color: white; 
                padding: 20px; 
                border-radius: 8px 8px 0 0; 
            }
            .chat-area { 
                height: 400px; 
                overflow-y: auto; 
                padding: 20px; 
                border-bottom: 1px solid #eee; 
            }
            .input-area { 
                padding: 20px; 
            }
            .file-upload { 
                margin-bottom: 15px; 
            }
            .message-input { 
                display: flex; 
                gap: 10px; 
            }
            input[type="text"] { 
                flex: 1; 
                padding: 10px; 
                border: 1px solid #ddd; 
                border-radius: 4px; 
            }
            button { 
                padding: 10px 20px; 
                background: #2563eb; 
                color: white; 
                border: none; 
                border-radius: 4px; 
                cursor: pointer; 
            }
            button:hover { 
                background: #1d4ed8; 
            }
            .message { 
                margin-bottom: 15px; 
                padding: 10px; 
                border-radius: 8px; 
            }
            .user-message { 
                background: #e0f2fe; 
                margin-left: 20%; 
            }
            .bot-message { 
                background: #f3f4f6; 
                margin-right: 20%; 
            }
            .code-block { 
                background: #1f2937; 
                color: #f9fafb; 
                padding: 15px; 
                border-radius: 6px; 
                margin: 10px 0; 
                overflow-x: auto; 
                font-family: 'Courier New', monospace; 
            }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>LLM ETL Chat Application</h1>
                <p>Upload files to S3 and generate ETL code for Snowflake ingestion</p>
            </div>
            <div class="chat-area" id="chatArea">
                <div class="message bot-message">
                    <strong>Assistant:</strong> Hello! I can help you upload files to S3 and generate ETL code for Snowflake ingestion. Please upload a file and describe what you'd like to do with it.
                </div>
            </div>
            <div class="input-area">
                <div class="file-upload">
                    <input type="file" id="fileInput" accept=".csv,.json,.xlsx,.xls,.txt,.parquet">
                    <button onclick="uploadFile()">Upload File</button>
                </div>
                <div class="message-input">
                    <input type="text" id="messageInput" placeholder="Describe your ETL requirements..." onkeypress="handleKeyPress(event)">
                    <button onclick="sendMessage()">Send</button>
                </div>
            </div>
        </div>

        <script>
            let currentFileInfo = null;

            async function uploadFile() {
                const fileInput = document.getElementById('fileInput');
                const file = fileInput.files[0];
                
                if (!file) {
                    alert('Please select a file');
                    return;
                }
                
                const formData = new FormData();
                formData.append('file', file);
                
                try {
                    const response = await fetch('/upload', {
                        method: 'POST',
                        body: formData
                    });
                    
                    if (response.ok) {
                        currentFileInfo = await response.json();
                        addMessage('user', `File uploaded: ${file.name}`);
                        
                        let uploadMessage = `File successfully uploaded to S3: ${currentFileInfo.s3_url}`;
                        
                        // Display data profiling results if available
                        if (currentFileInfo.data_profiling && currentFileInfo.data_profiling.success) {
                            uploadMessage += `\n\n📊 **Data Profile Analysis:**`;
                            
                            const profiling = currentFileInfo.data_profiling;
                            const datasetInfo = profiling.dataset_info || {};
                            const dataQuality = profiling.data_quality || {};
                            
                            // Basic dataset info
                            uploadMessage += `\n• Dataset: ${datasetInfo.rows || 'N/A'} rows × ${datasetInfo.columns || 'N/A'} columns`;
                            
                            // Data quality
                            if (dataQuality.summary) {
                                const completeness = dataQuality.summary.overall_completeness || 0;
                                const qualityIcon = completeness >= 95 ? "🟢" : completeness >= 80 ? "🟡" : "🔴";
                                uploadMessage += `\n• ${qualityIcon} Data Quality: ${completeness.toFixed(1)}% complete (${dataQuality.summary.data_size_mb || 'N/A'} MB)`;
                            }
                            
                            // Primary keys
                            if (profiling.primary_key_candidates && profiling.primary_key_candidates.length > 0) {
                                const pkList = profiling.primary_key_candidates.map(pk => {
                                    const column = pk.column || pk;
                                    const confidence = pk.confidence || 'unknown';
                                    const icon = confidence === 'high' ? '🔑' : '🗝️';
                                    return `${icon} ${column}`;
                                }).join(', ');
                                uploadMessage += `\n• Primary Key Candidates: ${pkList}`;
                            }
                            
                            // Date columns
                            if (profiling.date_columns && profiling.date_columns.length > 0) {
                                const dateList = profiling.date_columns.map(dc => {
                                    const column = dc.column || dc;
                                    const confidence = dc.confidence || 'unknown';
                                    const icon = confidence === 'high' ? '📅' : '📆';
                                    return `${icon} ${column}`;
                                }).join(', ');
                                uploadMessage += `\n• Date/Time Columns: ${dateList}`;
                            }
                            
                            // Data quality issues
                            if (dataQuality.completeness) {
                                const issues = [];
                                const warnings = [];
                                
                                Object.entries(dataQuality.completeness).forEach(([col, info]) => {
                                    if (info.status === 'poor') {
                                        issues.push(`🔴 ${col} (${info.null_percentage}% nulls)`);
                                    } else if (info.status === 'warning') {
                                        warnings.push(`🟡 ${col} (${info.null_percentage}% nulls)`);
                                    }
                                });
                                
                                if (issues.length > 0) {
                                    uploadMessage += `\n• Data Quality Issues: ${issues.join(', ')}`;
                                } else if (warnings.length > 0) {
                                    uploadMessage += `\n• Data Quality Warnings: ${warnings.join(', ')}`;
                                }
                            }
                            
                            // Schema info
                            if (profiling.schema_recommendations && profiling.schema_recommendations.columns) {
                                uploadMessage += `\n• ✅ Schema Ready: ${profiling.schema_recommendations.columns.length} columns mapped to Snowflake types`;
                            }
                            
                            uploadMessage += `\n\n🤖 **AI Insights:**`;
                            if (profiling.llm_insights) {
                                uploadMessage += `\n${profiling.llm_insights.substring(0, 300)}${profiling.llm_insights.length > 300 ? '...' : ''}`;
                            }
                            
                            uploadMessage += `\n\n💡 **Ready for ETL Code Generation!** \nDescribe your requirements and I'll generate optimized code based on this analysis.`;
                        } else if (currentFileInfo.data_profiling && !currentFileInfo.data_profiling.success) {
                            uploadMessage += `\n\n⚠️ Data profiling encountered an issue: ${currentFileInfo.data_profiling.error}`;
                            uploadMessage += `\nETL code generation will still work with basic file information.`;
                        }
                        
                        addMessage('bot', uploadMessage);
                    } else {
                        throw new Error('Upload failed');
                    }
                } catch (error) {
                    addMessage('bot', `Upload failed: ${error.message}`);
                }
            }
            
            async function sendMessage() {
                const messageInput = document.getElementById('messageInput');
                const message = messageInput.value.trim();
                
                if (!message) return;
                
                addMessage('user', message);
                messageInput.value = '';
                
                try {
                    const response = await fetch('/chat', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json'
                        },
                        body: JSON.stringify({
                            message: message,
                            file_url: currentFileInfo?.s3_url,
                            file_name: currentFileInfo?.original_filename
                        })
                    });
                    
                    if (response.ok) {
                        const result = await response.json();
                        addMessage('bot', result.response);
                        
                        if (result.etl_code) {
                            addCodeBlock(result.etl_code);
                        }
                    } else {
                        throw new Error('Chat request failed');
                    }
                } catch (error) {
                    addMessage('bot', `Error: ${error.message}`);
                }
            }
            
            function addMessage(sender, message) {
                const chatArea = document.getElementById('chatArea');
                const messageDiv = document.createElement('div');
                messageDiv.className = `message ${sender}-message`;
                messageDiv.innerHTML = `<strong>${sender === 'user' ? 'You' : 'Assistant'}:</strong> ${message}`;
                chatArea.appendChild(messageDiv);
                chatArea.scrollTop = chatArea.scrollHeight;
            }
            
            function addCodeBlock(code) {
                const chatArea = document.getElementById('chatArea');
                const codeDiv = document.createElement('div');
                codeDiv.className = 'code-block';
                codeDiv.textContent = code;
                chatArea.appendChild(codeDiv);
                chatArea.scrollTop = chatArea.scrollHeight;
            }
            
            function handleKeyPress(event) {
                if (event.key === 'Enter') {
                    sendMessage();
                }
            }
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    """Upload file to S3 and perform data profiling"""
    try:
        if not file.filename:
            raise HTTPException(status_code=400, detail="No file provided")
        
        if not file.filename.strip():
            raise HTTPException(status_code=400, detail="Invalid filename")
        
        # Upload to S3
        file_info = await upload_to_s3(file)
        
        # Perform data profiling for CSV files
        if file.filename.lower().endswith('.csv'):
            try:
                llm_generator = LLMCodeGenerator()
                profiling_result = llm_generator.profile_data_from_s3(
                    s3_url=file_info["s3_url"], 
                    bucket_name=Config.S3_BUCKET_NAME
                )
                file_info["data_profiling"] = profiling_result
            except Exception as e:
                print(f"Data profiling error: {str(e)}")
                file_info["data_profiling"] = {
                    "success": False,
                    "error": f"Data profiling failed: {str(e)}"
                }
        
        return JSONResponse(content=file_info)
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Upload endpoint error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

@app.post("/chat", response_model=ChatResponse)
async def chat(message: ChatMessage):
    """Handle chat messages and generate ETL code with data profiling insights"""
    try:
        if message.file_url:
            # Generate ETL code for the uploaded file with profiling data
            file_info = {
                "s3_url": message.file_url,
                "original_filename": message.file_name or "unknown.csv",
                "content_type": "application/octet-stream"
            }
            
            # Get data profiling insights if available
            profiling_data = None
            if message.file_name and message.file_name.lower().endswith('.csv'):
                try:
                    llm_generator = LLMCodeGenerator()
                    profiling_data = llm_generator.profile_data_from_s3(
                        s3_url=message.file_url, 
                        bucket_name=Config.S3_BUCKET_NAME
                    )
                except Exception as e:
                    print(f"Error getting profiling data for chat: {str(e)}")
            
            # Generate enhanced ETL code with profiling insights
            enhanced_requirements = message.message
            if profiling_data and profiling_data.get("success"):
                etl_code = llm_generator.generate_enhanced_etl_code(file_info, enhanced_requirements, profiling_data)
            else:
                etl_code = generate_etl_code(file_info, enhanced_requirements)
            
            response_parts = [
                f"I've analyzed your file ({message.file_name}) and generated comprehensive ETL code for Snowflake ingestion."
            ]
            
            if profiling_data and profiling_data.get("success"):
                dataset_info = profiling_data.get("dataset_info", {})
                response_parts.extend([
                    f"\n📊 **Data Profile Summary:**",
                    f"- Rows: {dataset_info.get('rows', 'N/A')}",
                    f"- Columns: {dataset_info.get('columns', 'N/A')}",
                    f"- Data Quality: {profiling_data.get('data_quality', {}).get('summary', {}).get('overall_completeness', 'N/A')}% complete"
                ])
                
                if profiling_data.get("primary_key_candidates"):
                    pk_candidates = [pk["column"] for pk in profiling_data["primary_key_candidates"]]
                    response_parts.append(f"- Primary Key Candidates: {', '.join(pk_candidates)}")
                
                if profiling_data.get("date_columns"):
                    date_cols = [dc["column"] for dc in profiling_data["date_columns"]]
                    response_parts.append(f"- Date/Time Columns: {', '.join(date_cols)}")
            
            response_parts.extend([
                f"\n🔧 **Generated ETL Features:**",
                "1. S3 file reading with error handling",
                "2. Data type inference and validation", 
                "3. Optimized Snowflake table creation",
                "4. Efficient data loading strategy",
                "5. Data quality checks and monitoring",
                "6. Comprehensive error handling and logging",
                "\nPlease review the generated code below:"
            ])
            
            response_text = "\n".join(response_parts)
            
            return ChatResponse(
                response=response_text,
                etl_code=etl_code,
                execution_status="generated"
            )
        else:
            # Regular chat without file using Nova Micro
            prompt = f"""
            You are a helpful assistant specializing in ETL processes, data engineering, and Snowflake. 
            Help users with their data ingestion questions.
            
            User question: {message.message}
            """
            
            response_text = llm_generator._invoke_bedrock_model(
                prompt=prompt,
                system_prompt="You are a helpful assistant specializing in ETL processes, data engineering, and Snowflake.",
                max_tokens=500
            )
            
            return ChatResponse(
                response=response_text,
                etl_code=None,
                execution_status=None
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Chat processing failed: {str(e)}")

@app.post("/profile-data")
async def profile_data(request: dict):
    """Standalone data profiling endpoint"""
    try:
        s3_url = request.get("s3_url")
        if not s3_url:
            raise HTTPException(status_code=400, detail="S3 URL is required")
        
        llm_generator = LLMCodeGenerator()
        profiling_result = llm_generator.profile_data_from_s3(
            s3_url=s3_url, 
            bucket_name=Config.S3_BUCKET_NAME
        )
        
        return JSONResponse(content=profiling_result)
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Data profiling endpoint error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Data profiling failed: {str(e)}")

@app.post("/etl-workflow", response_model=ETLWorkflowResponse)
async def run_etl_workflow_endpoint(request: ETLWorkflowRequest):
    """Run the complete ETL workflow using LangGraph"""
    try:
        print(f"🚀 Starting ETL workflow for file: {request.file_name}")
        
        # Prepare file info
        file_info = {
            "s3_url": request.file_url,
            "original_filename": request.file_name,
            "content_type": "application/octet-stream"
        }
        
        # Get data profiling insights first if it's a CSV
        profiling_data = None
        if request.file_name.lower().endswith('.csv'):
            try:
                profiling_data = llm_generator.profile_data_from_s3(
                    s3_url=request.file_url,
                    bucket_name=Config.S3_BUCKET_NAME
                )
                print(f"📊 Data profiling completed: {profiling_data.get('success', False)}")
            except Exception as e:
                print(f"⚠️ Profiling failed, continuing without insights: {str(e)}")
        
        # Run the LangGraph workflow
        workflow_result = run_etl_workflow(
            file_info=file_info,
            user_requirements=request.requirements,
            profiling_data=profiling_data
        )
        
        return ETLWorkflowResponse(
            success=workflow_result["success"],
            workflow_id=workflow_result.get("workflow_id"),
            script_path=workflow_result.get("script_path"),
            execution_success=workflow_result.get("execution_success"),
            snowflake_success=workflow_result.get("snowflake_success"),
            records_inserted=workflow_result.get("records_inserted"),
            execution_output=workflow_result.get("execution_output"),
            errors=workflow_result.get("errors"),
            summary=workflow_result.get("summary"),
            timestamp=workflow_result["timestamp"]
        )
        
    except Exception as e:
        error_msg = f"ETL workflow failed: {str(e)}"
        print(f"❌ {error_msg}")
        return ETLWorkflowResponse(
            success=False,
            errors={"workflow_error": error_msg},
            timestamp=datetime.now().isoformat()
        )

@app.get("/workflow-status/{workflow_id}")
async def get_workflow_status(workflow_id: str):
    """Get the status of a specific workflow"""
    try:
        from pathlib import Path
        scripts_dir = Path("generated_scripts")
        log_file = scripts_dir / f"{workflow_id}_workflow_log.json"
        
        if log_file.exists():
            with open(log_file, 'r') as f:
                workflow_log = json.load(f)
            return JSONResponse(content=workflow_log)
        else:
            raise HTTPException(status_code=404, detail="Workflow not found")
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get workflow status: {str(e)}")

@app.get("/workflows")
async def list_workflows():
    """List all available workflows"""
    try:
        from pathlib import Path
        scripts_dir = Path("generated_scripts")
        
        if not scripts_dir.exists():
            return {"workflows": []}
        
        workflows = []
        for log_file in scripts_dir.glob("*_workflow_log.json"):
            try:
                with open(log_file, 'r') as f:
                    workflow_log = json.load(f)
                    workflows.append({
                        "workflow_id": workflow_log.get("workflow_id"),
                        "timestamp": workflow_log.get("timestamp"),
                        "status": workflow_log.get("status"),
                        "execution_success": workflow_log.get("execution_success"),
                        "snowflake_success": workflow_log.get("snowflake_table_created"),
                        "records_inserted": workflow_log.get("snowflake_records_inserted", 0)
                    })
            except Exception as e:
                print(f"Error reading workflow log {log_file}: {e}")
                continue
        
        # Sort by timestamp (most recent first)
        workflows.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
        
        return {"workflows": workflows}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list workflows: {str(e)}")

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.get("/debug/config")
async def debug_config():
    """Debug configuration endpoint"""
    config_status = {
        "aws_access_key_configured": bool(Config.AWS_ACCESS_KEY_ID),
        "aws_secret_key_configured": bool(Config.AWS_SECRET_ACCESS_KEY),
        "s3_bucket_configured": bool(Config.S3_BUCKET_NAME),
        "aws_region": Config.AWS_REGION,
        "bedrock_region": Config.BEDROCK_REGION,
        "max_file_size_mb": Config.MAX_FILE_SIZE / (1024 * 1024),
        "allowed_extensions": Config.ALLOWED_EXTENSIONS,
        "environment": Config.ENVIRONMENT
    }
    return config_status

@app.get("/config")
async def get_config():
    """Get application configuration (non-sensitive)"""
    return {
        "aws_region": Config.AWS_REGION,
        "s3_bucket": Config.S3_BUCKET_NAME,
        "bedrock_region": Config.BEDROCK_REGION,
        "nova_model": Config.NOVA_MODEL_ID,
        "environment": Config.ENVIRONMENT
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
