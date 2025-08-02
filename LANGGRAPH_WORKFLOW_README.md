# LangGraph ETL Workflow Integration

This document explains the new LangGraph-based ETL workflow that automatically generates, saves, runs Python scripts, and ingests data to Snowflake.

## Overview

The LangGraph ETL Workflow provides an end-to-end automation solution that:

1. **Generates** Python ETL scripts using LLM with data profiling insights
2. **Saves** the generated scripts to disk with proper formatting
3. **Executes** the scripts automatically with environment configuration
4. **Ingests** data to Snowflake and validates the results
5. **Reports** comprehensive execution status and metrics

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Frontend      │    │   FastAPI        │    │   LangGraph     │
│   React App     │───▶│   Backend        │───▶│   Workflow      │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                         │
                       ┌─────────────────┐               │
                       │   Generated     │◀──────────────┘
                       │   Python Script │
                       └─────────────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │   Snowflake     │
                       │   Database      │
                       └─────────────────┘
```

## LangGraph Workflow Steps

### 1. Initialize Workflow
- Creates unique workflow ID
- Sets up metadata and tracking
- Prepares execution environment

### 2. Profile Data
- Analyzes uploaded data for insights
- Identifies primary keys and date columns
- Assesses data quality metrics
- Generates schema recommendations

### 3. Generate Script
- Uses LLM (AWS Bedrock Nova Micro) to generate ETL code
- Incorporates data profiling insights
- Injects environment configuration
- Optimizes for Snowflake ingestion

### 4. Save Script
- Writes generated script to `generated_scripts/` directory
- Creates executable files with proper permissions
- Maintains version control and tracking

### 5. Execute Script
- Runs the generated Python script
- Provides all necessary environment variables
- Captures execution output and errors
- Monitors execution with timeout protection

### 6. Validate Ingestion
- Connects to Snowflake to verify results
- Counts inserted records
- Validates table creation
- Reports ingestion status

### 7. Finalize Workflow
- Generates comprehensive summary
- Saves execution log as JSON
- Provides actionable results

## API Endpoints

### POST /etl-workflow
Runs the complete ETL workflow.

**Request:**
```json
{
  "file_url": "s3://bucket/file.csv",
  "file_name": "data.csv",
  "requirements": "Create ETL pipeline with data validation",
  "auto_execute": true
}
```

**Response:**
```json
{
  "success": true,
  "workflow_id": "etl_20250802_143022",
  "script_path": "generated_scripts/etl_20250802_143022_etl_script.py",
  "execution_success": true,
  "snowflake_success": true,
  "records_inserted": 1500,
  "summary": "Workflow completed successfully..."
}
```

### GET /workflow-status/{workflow_id}
Gets the detailed status of a specific workflow.

### GET /workflows
Lists all executed workflows with their status.

## Frontend Integration

### New "Run Workflow" Button
- Appears when a file is uploaded
- Triggers the complete LangGraph workflow
- Shows real-time execution status
- Displays comprehensive results

### Workflow Status Display
- Real-time progress indicators
- Execution output visualization
- Error reporting and diagnostics
- Success metrics and summaries

## Configuration Requirements

### Environment Variables
```bash
# AWS Configuration
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_REGION=us-east-1
S3_BUCKET_NAME=your-bucket

# Snowflake Configuration
SNOWFLAKE_ACCOUNT=your-account
SNOWFLAKE_USER=your-user
SNOWFLAKE_PASSWORD=your-password
SNOWFLAKE_WAREHOUSE=your-warehouse
SNOWFLAKE_DATABASE=your-database
SNOWFLAKE_SCHEMA=your-schema

# Bedrock Configuration
BEDROCK_REGION=us-east-1
NOVA_MODEL_ID=amazon.nova-micro-v1:0
```

## Generated Scripts Structure

### Script Components
1. **Imports and Configuration**
   - All necessary Python libraries
   - Environment variable loading
   - Logging configuration

2. **Data Profiling Integration**
   - Primary key handling
   - Date column processing
   - Data quality validation

3. **ETL Processing**
   - S3 data extraction
   - Data transformation logic
   - Snowflake table creation
   - Efficient data loading

4. **Error Handling**
   - Comprehensive exception handling
   - Detailed logging
   - Rollback mechanisms

### Example Generated Script
```python
import boto3
import pandas as pd
import snowflake.connector
import logging
from datetime import datetime

# Configuration (injected by LangGraph)
SNOWFLAKE_CONFIG = {
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'user': os.getenv('SNOWFLAKE_USER'),
    # ... more config
}

class ETLProcessor:
    def __init__(self):
        self.setup_logging()
        self.setup_connections()
    
    def process_data(self):
        # Generated ETL logic based on requirements
        # and data profiling insights
        pass

if __name__ == "__main__":
    processor = ETLProcessor()
    processor.process_data()
```

## Workflow Outputs

### Execution Logs
- Detailed JSON logs in `generated_scripts/`
- Execution timestamps and duration
- Success/failure status
- Error messages and stack traces

### Generated Scripts
- Python files with timestamp-based naming
- Executable permissions set automatically
- Environment configuration injected
- Production-ready code structure

### Snowflake Validation
- Table creation verification
- Record count validation
- Data type verification
- Constraint validation

## Error Handling

### Workflow-Level Errors
- Configuration validation
- Network connectivity issues
- Permission problems
- Resource constraints

### Script-Level Errors
- Data format issues
- Snowflake connection problems
- Data quality failures
- Processing timeouts

### Recovery Mechanisms
- Automatic retry logic
- Rollback capabilities
- Detailed error reporting
- Actionable error messages

## Testing

### Test Script Usage
```bash
python test_langgraph_workflow.py
```

### Test Components
1. **Configuration Validation**
   - Verifies all required environment variables
   - Tests connection to external services

2. **Workflow Component Testing**
   - Individual node testing
   - Graph structure validation
   - State management verification

3. **Full Workflow Testing**
   - End-to-end execution
   - Result validation
   - Performance monitoring

## Performance Considerations

### Optimization Features
- Parallel processing where possible
- Efficient data batching
- Memory-optimized operations
- Connection pooling

### Monitoring
- Execution time tracking
- Resource usage monitoring
- Success rate metrics
- Error frequency analysis

### Scaling
- Configurable timeout values
- Batch size optimization
- Connection pool sizing
- Memory management

## Security

### Credential Management
- Environment variable injection
- No hardcoded secrets
- Secure credential passing
- Temporary credential cleanup

### Access Control
- S3 bucket permissions
- Snowflake role-based access
- Network security considerations
- Audit trail maintenance

## Troubleshooting

### Common Issues
1. **Missing Configuration**
   - Check environment variables
   - Verify credential validity
   - Test network connectivity

2. **Script Execution Failures**
   - Review execution logs
   - Check Snowflake permissions
   - Validate data formats

3. **Snowflake Connection Issues**
   - Verify account details
   - Check warehouse status
   - Validate network access

### Debug Commands
```bash
# Test configuration
python test_langgraph_workflow.py

# Check workflow logs
ls -la generated_scripts/

# Validate Snowflake connection
python -c "import snowflake.connector; print('Connection test')"
```

## Future Enhancements

### Planned Features
- Workflow scheduling
- Advanced monitoring dashboard
- Multi-format file support
- Custom transformation templates
- Workflow versioning
- Performance analytics

### Integration Possibilities
- Apache Airflow integration
- dbt integration
- Data lineage tracking
- Quality monitoring alerts
- Custom notification systems

---

For more information about specific components, see:
- [LangGraph Documentation](https://python.langchain.com/docs/langgraph)
- [AWS Bedrock Nova Models](https://docs.aws.amazon.com/bedrock/)
- [Snowflake Python Connector](https://docs.snowflake.com/en/user-guide/python-connector.html)
