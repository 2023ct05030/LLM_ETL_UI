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
                        addMessage('bot', `File successfully uploaded to S3: ${currentFileInfo.s3_url}`);
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
    """Upload file to S3"""
    try:
        if not file.filename:
            raise HTTPException(status_code=400, detail="No file provided")
        
        if not file.filename.strip():
            raise HTTPException(status_code=400, detail="Invalid filename")
        
        file_info = await upload_to_s3(file)
        return JSONResponse(content=file_info)
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Upload endpoint error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

@app.post("/chat", response_model=ChatResponse)
async def chat(message: ChatMessage):
    """Handle chat messages and generate ETL code"""
    try:
        if message.file_url:
            # Generate ETL code for the uploaded file
            file_info = {
                "s3_url": message.file_url,
                "original_filename": message.file_name or "unknown.csv",
                "content_type": "application/octet-stream"
            }
            
            etl_code = generate_etl_code(file_info, message.message)
            
            response_text = f"I've generated ETL code to ingest your file ({message.file_name}) into Snowflake. The code includes:\n\n1. S3 file reading\n2. Data processing\n3. Snowflake table creation\n4. Data loading\n5. Error handling\n\nPlease review the generated code below:"
            
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
