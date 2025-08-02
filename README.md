# LLM ETL Frontend

A modern web application that combines LLM-powered chat with ETL code generation for seamless data ingestion from S3 to Snowflake, featuring automated LangGraph workflows.

## Features

🚀 **File Upload to S3**: Drag-and-drop interface for uploading CSV, JSON, Excel, and Parquet files  
🤖 **AWS Nova Micro LLM**: Generate production-ready ETL Python code using AWS Bedrock Nova Micro model  
🔄 **LangGraph Workflows**: Automated end-to-end ETL execution with data profiling  
❄️ **Snowflake Integration**: Automated table creation and data loading with validation  
💬 **Interactive Chat Interface**: Natural language interaction for ETL requirements  
📊 **Data Analysis**: Intelligent recommendations for data validation and transformations  
🎨 **Modern UI**: React + Material-UI for a professional user experience  
⚡ **Auto-Execution**: Generate, save, run, and validate ETL scripts automatically  

## New: LangGraph ETL Workflow

🔥 **Complete Automation**: The new LangGraph workflow provides end-to-end ETL automation:

1. **Data Profiling** - Automatically analyzes uploaded data for insights
2. **Script Generation** - Creates optimized Python ETL scripts with LLM
3. **Auto-Execution** - Runs generated scripts with proper configuration
4. **Snowflake Ingestion** - Loads data and validates results
5. **Comprehensive Reporting** - Provides detailed execution status and metrics

See [LANGGRAPH_WORKFLOW_README.md](./LANGGRAPH_WORKFLOW_README.md) for detailed documentation.

## Architecture

### Backend (FastAPI)
- **File Upload**: Secure S3 upload with UUID-based naming
- **AWS Bedrock Integration**: Nova Micro model for intelligent code generation
- **LangGraph Orchestration**: Multi-step workflow automation
- **Snowflake Connector**: Direct database operations with validation
- **RESTful API**: Clean endpoints for frontend communication

### Frontend (React + TypeScript)
- **Drag & Drop**: Intuitive file upload with react-dropzone
- **Chat Interface**: Real-time messaging with code highlighting
- **Workflow Controls**: Run complete ETL workflows with one click
- **Material-UI**: Professional component library
- **Code Display**: Syntax highlighting with copy/download features

## Quick Start

### Prerequisites
- Python 3.8+
- Node.js 16+
- AWS account with S3 and Bedrock access
- Snowflake account

### Installation & Setup

1. **Clone and navigate to the project**:
   ```bash
   cd /Users/ace/Working/llm_etl_fontend
   ```

2. **Configure environment variables**:
   ```bash
   cp .env.example .env
   # Edit .env with your actual credentials
   ```

3. **Start the application**:
   ```bash
   ./start.sh
   ```

This will:
- Create a Python virtual environment
- Install all dependencies
- Start the backend server on port 8000
- Start the frontend development server on port 3000

### Manual Setup

#### Backend Setup
```bash
# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Start server
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

#### Frontend Setup
```bash
cd frontend
npm install
npm start
```

## Environment Configuration

Copy `.env.example` to `.env` and configure:

```env
# AWS Credentials
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_REGION=us-east-1
S3_BUCKET_NAME=your-s3-bucket-name

# AWS Bedrock Configuration
BEDROCK_REGION=us-east-1
NOVA_MODEL_ID=amazon.nova-micro-v1:0

# Snowflake Configuration
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
```

## Usage

### Standard Workflow
1. **Access the application**: http://localhost:3000
2. **Upload a file**: Drag and drop or click to select a file
3. **Describe requirements**: Use natural language to specify your ETL needs
4. **Generate code**: The LLM will create custom Python ETL code
5. **Execute**: Copy or download the generated code for execution

### LangGraph Automated Workflow (NEW)
1. **Upload a file**: Same as above
2. **Describe requirements**: Specify your ETL needs in natural language
3. **Click "Run Workflow"**: Triggers the complete automated process
4. **Monitor progress**: View real-time execution status
5. **Review results**: Get comprehensive execution report and Snowflake validation

The automated workflow will:
- ✅ Profile your data automatically
- ✅ Generate optimized ETL script
- ✅ Execute the script safely
- ✅ Load data to Snowflake
- ✅ Validate ingestion results
- ✅ Provide detailed reporting

## API Endpoints

### Core Endpoints
- `GET /` - Main application interface
- `POST /upload` - File upload to S3
- `POST /chat` - Chat with LLM for code generation
- `GET /health` - Health check
- `GET /docs` - API documentation (Swagger UI)

### LangGraph Workflow Endpoints (NEW)
- `POST /etl-workflow` - Run complete automated ETL workflow
- `GET /workflow-status/{workflow_id}` - Get workflow execution status
- `GET /workflows` - List all executed workflows with status

### Data Analysis Endpoints
- `POST /profile-data` - Get data profiling insights for uploaded files

## Supported File Types

- **CSV** (.csv)
- **JSON** (.json)
- **Excel** (.xlsx, .xls)
- **Parquet** (.parquet)
- **Text** (.txt)

## Generated ETL Code Features

✅ S3 file reading with boto3  
✅ Automatic schema inference  
✅ Snowflake table creation  
✅ Data type mapping  
✅ Error handling and logging  
✅ Progress tracking  
✅ Data validation  
✅ Environment variable configuration  

## Development

### Project Structure
```
llm_etl_fontend/
├── main.py              # FastAPI application
├── etl_processor.py     # ETL processing utilities
├── llm_generator.py     # LLM code generation
├── requirements.txt     # Python dependencies
├── frontend/            # React application
│   ├── src/
│   │   ├── App.tsx      # Main React component
│   │   └── index.tsx    # Application entry point
│   └── package.json     # Node.js dependencies
└── start.sh            # Startup script
```

### Adding New Features

1. **Backend**: Add new endpoints in `main.py`
2. **Frontend**: Extend components in `frontend/src/`
3. **ETL Logic**: Enhance `etl_processor.py`
4. **LLM Prompts**: Modify `llm_generator.py`

## Troubleshooting

### Common Issues

**Backend won't start**:
- Check Python version (3.8+)
- Verify virtual environment activation
- Review `.env` file configuration

**Frontend won't start**:
- Ensure Node.js 16+ is installed
- Delete `node_modules` and run `npm install`
- Check for port conflicts (3000)

**File upload fails**:
- Verify AWS S3 credentials
- Check S3 bucket permissions
- Ensure bucket exists and is accessible

**Snowflake connection fails**:
- Verify Snowflake credentials
- Check network connectivity
- Ensure warehouse is running

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and questions:
- Create an issue in the repository
- Check the API documentation at `/docs`
- Review the troubleshooting section

---

Built with ❤️ using FastAPI, React, and the power of LLMs