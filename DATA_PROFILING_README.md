# Data Profiling Integration - Feature Documentation

## Overview

The LLM ETL Generator now includes comprehensive data profiling functionality that automatically analyzes uploaded CSV files and provides intelligent recommendations for ETL development. This feature uses AWS Bedrock Nova Micro model to generate expert-level insights.

## What's New

### 🔧 **Automatic Data Profiling**
- **Triggered**: Automatically when CSV files are uploaded to S3
- **Analysis**: Comprehensive data quality, schema, and pattern analysis
- **AI-Powered**: Uses Nova Micro model for expert insights

### 📊 **Profiling Features**

#### **Primary Key Detection**
- Identifies columns with high uniqueness and low null rates
- Confidence scoring (high, medium, low)
- Handles composite key scenarios

#### **Date/Time Column Identification**  
- Detects datetime columns for SCD2 implementation
- Parses various date formats automatically
- Identifies audit trail columns (created_date, updated_date, etc.)

#### **Data Quality Analysis**
- **Completeness**: Null value analysis per column
- **Uniqueness**: Cardinality assessment  
- **Data Types**: Optimal type recommendations
- **Overall Score**: Comprehensive quality metric

#### **Schema Optimization**
- Snowflake-specific data type mapping
- Column sizing recommendations
- Constraint suggestions (NOT NULL, UNIQUE, etc.)

#### **AI-Powered Insights**
- Expert ETL recommendations
- Performance optimization tips
- Data governance considerations
- SCD2 strategy recommendations

## Usage

### 1. **File Upload with Profiling**

```javascript
// Frontend automatically receives profiling data
const uploadResponse = await fetch('/upload', {
    method: 'POST',
    body: formData
});

const result = await uploadResponse.json();
console.log(result.data_profiling); // Profiling results
```

### 2. **Enhanced ETL Code Generation**

```python
# Backend automatically uses profiling insights
llm_generator = LLMCodeGenerator()
profiling_data = llm_generator.profile_data_from_s3(s3_url, bucket_name)
etl_code = llm_generator.generate_enhanced_etl_code(file_info, requirements, profiling_data)
```

### 3. **Standalone Profiling**

```bash
# API endpoint for standalone profiling
POST /profile-data
{
    "s3_url": "s3://bucket/file.csv"
}
```

## Sample Output

### Upload Response with Profiling
```
📊 Data Profile Analysis:
• Dataset: 1000 rows × 8 columns
• 🟢 Data Quality: 98.5% complete (2.4 MB)
• Primary Key Candidates: 🔑 customer_id, 🗝️ email
• Date/Time Columns: 📅 signup_date, 📅 last_login
• ✅ Schema Ready: 8 columns mapped to Snowflake types

🤖 AI Insights:
This customer dataset shows excellent data quality with strong primary key candidates. 
The presence of signup_date and last_login suggests this is suitable for SCD2 
implementation to track customer evolution over time...

💡 Ready for ETL Code Generation!
Describe your requirements and I'll generate optimized code based on this analysis.
```

### Enhanced ETL Code Features
- **Smart table creation** using profiling insights
- **Optimized data types** based on content analysis
- **Primary key constraints** from candidate detection
- **Date parsing logic** for identified datetime columns
- **Data quality validations** from profiling results
- **Performance optimizations** based on dataset characteristics

## Configuration

### Required Environment Variables
```bash
# AWS Configuration (for S3 and Bedrock)
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-east-1
S3_BUCKET_NAME=your_bucket

# Bedrock Configuration
BEDROCK_REGION=us-east-1
NOVA_MODEL_ID=amazon.nova-micro-v1:0
```

### File Support
- **Supported**: CSV files (.csv)
- **Future**: Excel (.xlsx), JSON (.json), Parquet (.parquet)
- **Size Limit**: 100MB (configurable)

## API Endpoints

### 1. **Enhanced Upload**
```
POST /upload
- Uploads file to S3
- Performs automatic profiling for CSV files
- Returns file info + profiling results
```

### 2. **Standalone Profiling**
```
POST /profile-data
Body: {"s3_url": "s3://bucket/file.csv"}
- Profiles existing S3 file
- Returns comprehensive analysis
```

### 3. **Enhanced Chat**
```
POST /chat
- Uses profiling data for ETL generation
- Provides context-aware recommendations
- Generates optimized code
```

## Technical Architecture

### Data Flow
```
CSV Upload → S3 Storage → Pandas Analysis → Nova Model → Insights → ETL Code
```

### Components
1. **LLMCodeGenerator**: Core profiling and generation engine
2. **FastAPI Endpoints**: REST API for file handling
3. **Pandas Analytics**: Statistical analysis and pattern detection
4. **Nova Model**: AI-powered insights and recommendations
5. **React Frontend**: User interface with profiling display

## Benefits

### For Data Engineers
- **Faster Development**: Automated schema discovery
- **Better Quality**: Data quality insights upfront
- **Optimized Code**: Performance-tuned ETL scripts
- **Best Practices**: AI-recommended patterns

### For Data Teams
- **Visibility**: Immediate data quality assessment
- **Documentation**: Automatic data profiling reports
- **Governance**: Built-in quality monitoring
- **Collaboration**: Shareable profiling insights

## Future Enhancements

### Planned Features
- **Multi-format Support**: Excel, JSON, Parquet profiling
- **Custom Rules**: User-defined quality thresholds
- **Historical Profiling**: Track data quality over time
- **Advanced Analytics**: Statistical outlier detection
- **Data Lineage**: Automatic documentation generation

### Integration Opportunities
- **dbt Integration**: Generate dbt models from profiling
- **Great Expectations**: Auto-create data tests
- **Snowflake Native**: Direct integration with Snowflake profiling
- **MLOps**: Feature engineering recommendations

## Troubleshooting

### Common Issues

1. **Profiling Not Running**
   - Check AWS credentials configuration
   - Verify S3 bucket permissions
   - Ensure file is valid CSV format

2. **Incomplete Insights**
   - Check Bedrock model access
   - Verify Nova model availability in region
   - Review token limits and model quotas

3. **Performance Issues**
   - Large files may take longer to profile
   - Consider file size limits
   - Check memory usage for large datasets

### Debug Endpoints
```bash
GET /health - System health check
GET /debug/config - Configuration validation
```

## Example Integration

```python
from llm_generator import LLMCodeGenerator

# Initialize generator
llm_gen = LLMCodeGenerator()

# Profile uploaded file
profiling_result = llm_gen.profile_data_from_s3(
    s3_url="s3://my-bucket/data.csv",
    bucket_name="my-bucket"
)

# Generate enhanced ETL code
if profiling_result['success']:
    etl_code = llm_gen.generate_enhanced_etl_code(
        file_info={"s3_url": "s3://my-bucket/data.csv", "original_filename": "data.csv"},
        requirements="Load into Snowflake with SCD2",
        profiling_data=profiling_result
    )
    print(etl_code)
```

This data profiling integration transforms the ETL development experience by providing immediate, actionable insights about your data before you even write a single line of code.
