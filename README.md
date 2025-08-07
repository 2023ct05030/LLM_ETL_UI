# LLM ETL Generator - Comprehensive Documentation

A modern, AI-powered ETL (Extract, Transform, Load) platform that combines LLM-driven code generation with automated workflow orchestration for seamless data ingestion from S3 to Snowflake.

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Backend Components](#backend-components)
- [Frontend Components](#frontend-components)
- [Core Scripts Documentation](#core-scripts-documentation)
- [API Endpoints](#api-endpoints)
- [Configuration](#configuration)
- [Development Setup](#development-setup)
- [Testing](#testing)
- [Deployment](#deployment)
- [Troubleshooting](#troubleshooting)

## Architecture Overview

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Frontend      │    │   FastAPI        │    │   LangGraph     │
│   React App     │───▶│   Backend        │───▶│   Workflow      │
│  (TypeScript)   │    │   (Python)       │    │   Engine        │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                ┃                         ┃
                       ┌────────┗──────────┐              ┃
                       ▼                   ▼              ▼
               ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
               │   AWS S3     │  │   Snowflake  │  │  Generated   │
               │   Storage    │  │   Database   │  │  Python      │
               └──────────────┘  └──────────────┘  │  Scripts     │
                                                   └──────────────┘
                                                           ┃
                                                           ▼
                                                   ┌──────────────┐
                                                   │   AWS        │
                                                   │   Bedrock    │
                                                   │   Nova LLM   │
                                                   └──────────────┘
```

### Technology Stack

#### Backend
- **FastAPI**: Modern, fast web framework for building APIs
- **Python 3.8+**: Core programming language
- **LangGraph**: Workflow orchestration framework
- **AWS Bedrock**: AI/ML service for LLM integration
- **Pandas**: Data manipulation and analysis
- **Boto3**: AWS SDK for Python

#### Frontend
- **React 18**: Component-based UI library
- **TypeScript**: Type-safe JavaScript
- **Material-UI (MUI)**: Professional React component library
- **Axios**: HTTP client for API communication
- **React Dropzone**: File upload functionality

#### Infrastructure
- **AWS S3**: Object storage for file uploads
- **Snowflake**: Cloud data warehouse
- **Docker**: Containerization platform
- **uvicorn**: ASGI server for FastAPI

## Backend Components

### 1. Main Application (`main.py`)

**Purpose**: Central FastAPI application serving as the API gateway and primary backend service.

**Key Features**:
- File upload handling with S3 integration
- LLM-powered chat interface for ETL requirements
- RESTful API endpoints for frontend communication
- Workflow orchestration endpoints
- Health monitoring and configuration debugging

**Core Endpoints**:
```python
# File Operations
POST /upload           # Upload files to S3 with automatic profiling
POST /profile-data     # Standalone data profiling

# Chat Interface
POST /chat            # Natural language ETL requirement processing

# Workflow Management
POST /etl-workflow    # Execute complete automated ETL workflow
GET /workflow-status/{id}  # Check workflow execution status
GET /workflows        # List all executed workflows

# System
GET /health          # Health check endpoint
GET /config          # Application configuration
GET /docs           # Interactive API documentation
```

**Dependencies**:
- `config.py`: Configuration management
- `llm_generator.py`: LLM code generation
- `langgraph_etl_workflow.py`: Workflow orchestration

### 2. LLM Code Generator (`llm_generator.py`)

**Purpose**: AI-powered ETL code generation using AWS Bedrock Nova Micro model.

**Key Features**:
- **Data Profiling Engine**: Comprehensive analysis of uploaded datasets
  - Primary key candidate detection
  - Data quality assessment
  - Column type inference
  - Statistical analysis
  - Pattern recognition

- **Intelligent Code Generation**: Context-aware ETL script creation
  - S3 file reading with boto3
  - Snowflake table creation and optimization
  - Data type mapping and validation
  - Error handling and logging
  - Performance optimization

- **AI Insights**: Nova model-powered recommendations
  - Data transformation suggestions
  - Schema optimization advice
  - Best practice implementation

**Key Methods**:
```python
profile_data_from_s3()          # Main profiling orchestrator
_find_primary_key_candidates()   # Uniqueness analysis
_find_date_columns()            # Temporal pattern detection
_analyze_data_quality()         # Quality metrics calculation
_generate_llm_data_insights()   # AI-powered recommendations
generate_enhanced_etl_code()    # Context-aware code generation
```

### 3. LangGraph Workflow Engine (`langgraph_etl_workflow.py`)

**Purpose**: Complete end-to-end ETL workflow orchestration using LangGraph framework.

**Workflow Stages**:

1. **Data Profiling**: Automated analysis of uploaded files
2. **Script Generation**: AI-powered ETL code creation
3. **Script Execution**: Safe execution with monitoring
4. **Snowflake Ingestion**: Data loading and validation
5. **Results Reporting**: Comprehensive execution summary

**Key Features**:
- **State Management**: Comprehensive workflow state tracking
- **Error Recovery**: Graceful error handling and reporting
- **Monitoring**: Real-time execution progress tracking
- **Validation**: Post-ingestion data validation
- **Logging**: Detailed execution logs and metrics

**Workflow State Structure**:
```python
class ETLWorkflowState(TypedDict):
    # Input parameters
    file_info: Dict[str, Any]
    user_requirements: str
    profiling_data: Optional[Dict[str, Any]]
    
    # Generated content
    generated_script: str
    script_path: str
    
    # Execution results
    execution_output: str
    execution_success: bool
    execution_error: Optional[str]
    
    # Snowflake results
    snowflake_table_created: bool
    snowflake_records_inserted: int
    snowflake_error: Optional[str]
    
    # Workflow metadata
    workflow_id: str
    timestamp: str
    status: str
```

### 4. Data Profiling Agent (`dataprofiling.py`)

**Purpose**: Specialized LangGraph agent for identifying primary keys and date columns.

**Capabilities**:
- Primary/business key candidate identification
- SCD2 date column detection
- Statistical uniqueness analysis
- Data type inference
- Pattern recognition

### 5. ETL Processor (`etl_processor.py`)

**Purpose**: Core ETL processing utilities for file handling and Snowflake operations.

**Features**:
- Multi-format file reading (CSV, JSON, Excel, Parquet)
- Snowflake connection management
- Data type conversion and mapping
- Error handling and logging

### 6. Configuration Management (`config.py`)

**Purpose**: Centralized configuration management with environment variable support.

**Configuration Categories**:
```python
# AWS Configuration
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_REGION
S3_BUCKET_NAME

# AWS Bedrock Configuration
BEDROCK_REGION
NOVA_MODEL_ID

# Snowflake Configuration
SNOWFLAKE_ACCOUNT
SNOWFLAKE_USER
SNOWFLAKE_PASSWORD
SNOWFLAKE_WAREHOUSE
SNOWFLAKE_DATABASE
SNOWFLAKE_SCHEMA

# Application Settings
SECRET_KEY
ENVIRONMENT
DEBUG
MAX_FILE_SIZE
ALLOWED_EXTENSIONS
LOG_LEVEL
```

## Frontend Components

### 1. Main Application (`frontend/src/App.tsx`)

**Purpose**: Primary React component providing the user interface for the ETL platform.

**Key Features**:

#### File Upload Interface
- **Drag & Drop**: Intuitive file upload using react-dropzone
- **Multi-format Support**: CSV, JSON, Excel, Parquet files
- **Upload Progress**: Real-time upload status and progress indicators
- **File Validation**: Client-side validation for supported formats

#### Chat Interface
- **Natural Language Processing**: Describe ETL requirements in plain English
- **Real-time Messaging**: Instant communication with the backend
- **Message History**: Persistent chat history with timestamps
- **Rich Content Display**: Formatted text with code highlighting

#### Code Display & Management
- **Syntax Highlighting**: Python code display with syntax highlighting
- **Copy to Clipboard**: One-click code copying functionality
- **Download Code**: Save generated scripts as files
- **Code Execution**: Direct execution of generated ETL scripts

#### Workflow Management
- **One-Click Workflows**: Execute complete ETL workflows with single button
- **Progress Monitoring**: Real-time workflow execution tracking
- **Result Visualization**: Comprehensive workflow result display
- **Error Reporting**: Detailed error messages and troubleshooting

#### Data Profiling Display
- **Dataset Metrics**: Rows, columns, data size information
- **Quality Indicators**: Visual data quality scores and indicators
- **Primary Key Candidates**: Display of detected primary key candidates
- **Data Insights**: AI-generated insights and recommendations

**Component Structure**:
```typescript
interface Message {
  id: string;
  type: 'user' | 'assistant';
  content: string;
  timestamp: Date;
  etlCode?: string;
}

interface FileInfo {
  s3_url: string;
  original_filename: string;
  content_type: string;
  file_size: number;
  upload_timestamp: string;
  profiling?: any;
}
```

### 2. Application Entry Point (`frontend/src/index.tsx`)

**Purpose**: Application bootstrap and theme configuration.

**Features**:
- **Theme Configuration**: Dark theme with cyan accent colors
- **Material-UI Setup**: Component library initialization
- **React Root Mounting**: Application mounting and rendering

**Theme Configuration**:
```typescript
const theme = createTheme({
  palette: {
    mode: 'dark',
    primary: { main: '#00bcd4' },    // Cyan
    secondary: { main: '#00e5ff' },   // Light cyan
    background: {
      default: '#0a0a0a',            // Dark background
      paper: '#1a1a1a',              // Dark paper
    },
  },
});
```

### 3. Package Configuration (`frontend/package.json`)

**Dependencies**:

#### Core Framework
- `react`: ^18.2.0 - Component-based UI library
- `react-dom`: ^18.2.0 - React DOM rendering
- `typescript`: ^4.9.4 - Type-safe JavaScript

#### UI Components
- `@mui/material`: ^5.15.1 - Material Design components
- `@mui/icons-material`: ^5.15.1 - Material Design icons
- `@emotion/react`: ^11.11.1 - CSS-in-JS library
- `@emotion/styled`: ^11.11.0 - Styled components

#### Functionality
- `axios`: ^1.6.2 - HTTP client for API calls
- `react-dropzone`: ^14.2.3 - File upload component
- `react-syntax-highlighter`: ^15.6.1 - Code syntax highlighting

#### Development
- `react-scripts`: ^5.0.1 - Build tools and development server
- `@testing-library/*`: Testing utilities

**Scripts**:
```json
{
  "start": "react-scripts start",      // Development server
  "build": "react-scripts build",      // Production build
  "test": "react-scripts test",        // Test runner
  "eject": "react-scripts eject"       // Eject configuration
}
```

## Core Scripts Documentation

### 1. Application Startup (`start.sh`)

**Purpose**: Automated application startup script for development and testing.

**Functionality**:
```bash
# Environment Setup
- Creates Python virtual environment (.venv)
- Activates virtual environment
- Installs Python dependencies via uv

# Configuration
- Creates .env file from template if missing
- Validates environment configuration

# Service Startup
- Starts FastAPI backend server (port 8000)
- Installs frontend dependencies
- Starts React development server (port 3000)

# Process Management
- Background process management
- Graceful shutdown handling
- Error reporting and cleanup
```

**Usage**:
```bash
./start.sh
```

**Output Services**:
- Backend API: http://localhost:8000
- Frontend Application: http://localhost:3000
- API Documentation: http://localhost:8000/docs

### 2. Git Operations (`git_push.sh`)

**Purpose**: Automated git operations for version control.

**Functionality**:
- Stages all changes
- Commits with timestamp
- Pushes to remote repository

### 3. Environment Cleanup (`purge.sh`)

**Purpose**: Clean development environment and remove temporary files.

**Functionality**:
- Removes virtual environments
- Cleans build artifacts
- Removes temporary files
- Resets development state

### 4. Test Runners

#### LangGraph Workflow Testing (`test_langgraph_workflow.py`)
**Purpose**: Comprehensive testing of the LangGraph ETL workflow.

**Test Coverage**:
- Complete workflow execution
- Error handling and recovery
- State management validation
- Snowflake integration testing

#### Data Profiling Testing (`test_profiling.py`)
**Purpose**: Validation of data profiling functionality.

#### NaN Handling Testing (`test_nan_handling.py`)
**Purpose**: Testing edge cases with missing data and NaN values.

### 5. Integration Summary Generator (`create_integration_summary.py`)

**Purpose**: Automated documentation generation for integration summaries.

**Output Files**:
- `INTEGRATION_SUMMARY.md`: Markdown documentation
- `LANGGRAPH_INTEGRATION_SUMMARY.json`: JSON metadata
- `LANGGRAPH_INTEGRATION_SUMMARY.txt`: Plain text summary

## API Endpoints

### File Operations

#### POST /upload
**Purpose**: Upload files to S3 with automatic data profiling

**Request**:
```http
POST /upload
Content-Type: multipart/form-data

file: <binary-file-data>
```

**Response**:
```json
{
  "s3_url": "s3://bucket/file.csv",
  "original_filename": "data.csv",
  "content_type": "text/csv",
  "file_size": 1024000,
  "upload_timestamp": "2025-08-07T10:30:00",
  "profiling": {
    "dataset_info": {...},
    "data_quality": {...},
    "primary_key_candidates": [...],
    "date_columns": [...],
    "llm_insights": {...}
  }
}
```

#### POST /profile-data
**Purpose**: Standalone data profiling for existing S3 files

**Request**:
```json
{
  "s3_url": "s3://bucket/file.csv"
}
```

### Chat Interface

#### POST /chat
**Purpose**: Natural language ETL requirement processing

**Request**:
```json
{
  "message": "Create ETL pipeline for customer data",
  "file_url": "s3://bucket/file.csv",
  "file_name": "customers.csv"
}
```

**Response**:
```json
{
  "response": "I'll create an ETL pipeline...",
  "etl_code": "import pandas as pd\n..."
}
```

### Workflow Management

#### POST /etl-workflow
**Purpose**: Execute complete automated ETL workflow

**Request**:
```json
{
  "file_url": "s3://bucket/file.csv",
  "file_name": "data.csv",
  "requirements": "Create comprehensive ETL pipeline",
  "auto_execute": true
}
```

**Response**:
```json
{
  "workflow_id": "etl_20250807_140406",
  "success": true,
  "generated_script": "...",
  "execution_output": "...",
  "snowflake_results": {...},
  "profiling_data": {...},
  "summary": "Workflow completed successfully",
  "errors": {},
  "timestamp": "2025-08-07T14:04:06"
}
```

#### GET /workflow-status/{workflow_id}
**Purpose**: Check workflow execution status

#### GET /workflows
**Purpose**: List all executed workflows

### System Endpoints

#### GET /health
**Purpose**: Health check endpoint

**Response**:
```json
{
  "status": "healthy",
  "timestamp": "2025-08-07T10:30:00"
}
```

#### GET /config
**Purpose**: Application configuration (non-sensitive)

#### GET /debug/config
**Purpose**: Debug configuration with status indicators

## Configuration

### Environment Variables

Create a `.env` file in the project root with the following configuration:

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

# Application Settings (Optional)
SECRET_KEY=your-secret-key-change-in-production
ENVIRONMENT=development
MAX_FILE_SIZE=100  # MB
LOG_LEVEL=INFO
```

### Docker Configuration

#### Backend Dockerfile
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

#### Frontend Dockerfile
```dockerfile
FROM node:18-alpine
WORKDIR /app
COPY package*.json ./
RUN npm ci --only=production
COPY . .
RUN npm run build
EXPOSE 3000
CMD ["npm", "start"]
```

#### Docker Compose
```yaml
version: '3.8'
services:
  backend:
    build: .
    ports:
      - "8000:8000"
    environment:
      - AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
      - AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
      # ... other environment variables
  
  frontend:
    build: ./frontend
    ports:
      - "3000:3000"
    environment:
      - REACT_APP_API_URL=http://localhost:8000
    depends_on:
      - backend
  
  redis:
    image: redis:alpine
    ports:
      - "6379:6379"
```

## Development Setup

### Prerequisites
- **Python 3.8+**: Backend development
- **Node.js 16+**: Frontend development
- **AWS Account**: S3 and Bedrock access required
- **Snowflake Account**: Data warehouse access

### Quick Start

1. **Clone Repository**:
   ```bash
   git clone <repository-url>
   cd LLM_ETL_GEN_UI
   ```

2. **Configure Environment**:
   ```bash
   cp .env.example .env
   # Edit .env with your credentials
   ```

3. **Start Application**:
   ```bash
   ./start.sh
   ```

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

### Project Structure
```
LLM_ETL_GEN_UI/
├── main.py                      # FastAPI application
├── llm_generator.py             # LLM code generation
├── langgraph_etl_workflow.py    # Workflow orchestration
├── dataprofiling.py             # Data profiling agent
├── etl_processor.py             # ETL processing utilities
├── config.py                    # Configuration management
├── requirements.txt             # Python dependencies
├── pyproject.toml               # Project configuration
├── docker-compose.yml           # Container orchestration
├── Dockerfile                   # Backend container
├── start.sh                     # Startup script
├── .env.example                 # Environment template
├── frontend/                    # React application
│   ├── src/
│   │   ├── App.tsx              # Main React component
│   │   ├── index.tsx            # Application entry point
│   ├── package.json             # Frontend dependencies
│   ├── Dockerfile               # Frontend container
│   └── public/                  # Static assets
├── generated_scripts/           # Generated ETL scripts
├── __pycache__/                 # Python cache
└── docs/                        # Documentation
    ├── README.md                # Main documentation
    ├── LANGGRAPH_WORKFLOW_README.md
    ├── DATA_PROFILING_README.md
    └── INTEGRATION_SUMMARY.md
```

## Testing

### Test Files

#### Backend Testing
- `test_langgraph_workflow.py`: Complete workflow testing
- `test_profiling.py`: Data profiling validation
- `test_nan_handling.py`: Edge case testing

#### Running Tests
```bash
# Activate virtual environment
source .venv/bin/activate

# Run specific tests
python test_langgraph_workflow.py
python test_profiling.py
python test_nan_handling.py

# Run all tests (if pytest is configured)
pytest
```

#### Test Coverage
- **Workflow Execution**: End-to-end ETL workflow testing
- **Data Profiling**: Statistical analysis validation
- **Error Handling**: Edge case and error scenario testing
- **Integration**: AWS and Snowflake connectivity testing

### Frontend Testing
```bash
cd frontend
npm test
```

## Deployment

### Docker Deployment
```bash
# Build and start services
docker-compose up --build

# Production deployment
docker-compose -f docker-compose.prod.yml up -d
```

### Manual Deployment

#### Backend (Production)
```bash
# Install dependencies
pip install -r requirements.txt

# Start with production settings
uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4
```

#### Frontend (Production)
```bash
cd frontend
npm run build
npm install -g serve
serve -s build -l 3000
```

### Environment Configuration

#### Production Environment Variables
```env
ENVIRONMENT=production
DEBUG=false
SECRET_KEY=strong-production-secret-key
```

#### Security Considerations
- Use strong secret keys
- Implement proper CORS policies
- Use HTTPS in production
- Secure environment variable management
- Regular security updates

## Troubleshooting

### Common Issues

#### Backend Issues

**Problem**: Backend won't start
**Solutions**:
- Check Python version (3.8+ required)
- Verify virtual environment activation
- Review `.env` file configuration
- Check AWS credentials validity

**Problem**: AWS connection fails
**Solutions**:
- Verify AWS credentials
- Check IAM permissions for S3 and Bedrock
- Validate region configuration
- Test AWS connectivity

**Problem**: Snowflake connection fails
**Solutions**:
- Verify Snowflake credentials
- Check network connectivity
- Ensure warehouse is running
- Validate account identifier

#### Frontend Issues

**Problem**: Frontend won't start
**Solutions**:
- Ensure Node.js 16+ is installed
- Delete `node_modules` and run `npm install`
- Check for port conflicts (3000)
- Verify backend connectivity

**Problem**: File upload fails
**Solutions**:
- Verify AWS S3 credentials and permissions
- Check S3 bucket configuration
- Validate file size limits
- Review CORS settings

#### Workflow Issues

**Problem**: LangGraph workflow fails
**Solutions**:
- Check generated script validity
- Verify Snowflake connection
- Review execution logs
- Validate input data format

**Problem**: Data profiling errors
**Solutions**:
- Check file format compatibility
- Verify S3 file accessibility
- Review data quality
- Check memory usage for large files

### Debug Commands

#### Configuration Debugging
```bash
# Check configuration status
curl http://localhost:8000/debug/config

# Health check
curl http://localhost:8000/health
```

#### Log Analysis
```bash
# View backend logs
tail -f backend.log

# View workflow logs
cat generated_scripts/*_workflow_log.json
```

### Performance Optimization

#### Backend Optimization
- Use connection pooling for Snowflake
- Implement caching for repeated operations
- Optimize pandas operations for large datasets
- Use async operations where applicable

#### Frontend Optimization
- Implement code splitting
- Use React.memo for expensive components
- Optimize bundle size
- Implement proper error boundaries

## Support and Maintenance

### Monitoring
- Health check endpoints for service monitoring
- Comprehensive logging throughout the application
- Workflow execution tracking and reporting
- Error tracking and alerting

### Updates and Maintenance
- Regular dependency updates
- Security patch management
- Performance monitoring and optimization
- User feedback integration

### Contributing
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request


---

**Built with ❤️ using FastAPI, React, LangGraph, and the power of AWS Bedrock Nova LLM**
