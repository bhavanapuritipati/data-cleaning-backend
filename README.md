# Data Polisher Backend

An intelligent data cleaning and preprocessing API built with FastAPI, LangGraph, and AI-powered agents. The backend provides automated data quality assessment, missing value imputation, outlier detection, and data transformations.

**Live API**: [https://data-polisher.onrender.com](https://data-polisher.onrender.com)  
**API Documentation**: [https://data-polisher.onrender.com/docs](https://data-polisher.onrender.com/docs)  
**Frontend**: [https://data-polisher-ui.vercel.app/](https://data-polisher-ui.vercel.app/)

## Features

### Intelligent Data Analysis
- **Schema Validation**: Comprehensive schema analysis with LLM-powered domain detection
- **Data Type Classification**: Automatic detection of numerical, categorical, datetime, and text columns
- **Unit Detection**: Identification of units ($, %, etc.) in column names and values

### Data Cleaning Pipeline
- **Missing Value Imputation**: Smart imputation strategies based on data type and missing percentage
  - Median imputation for low missing numerical data
  - KNN imputation for moderate missing values
  - Mode imputation for categorical data
- **Outlier Detection**: Multi-method outlier detection using IQR, Z-score, and Isolation Forest
- **Data Transformations**: 
  - Currency column cleaning (removes $, commas, converts to numeric)
  - Year/Date column normalization
  - Text normalization and formatting
  - Percentage column conversion
  - Boolean column standardization

### Smart Column Management
- **Automatic Column Removal**: Removes columns based on LLM recommendations
  - High cardinality non-useful columns
  - High missingness (>70%) unusable columns
  - Reference IDs with no predictive value
- **Protected Columns**: Year/Date columns are protected from removal even with high cardinality
- **Encoding Support**: UTF-8-SIG encoding for Excel compatibility

### Real-time Processing
- **WebSocket Support**: Real-time progress updates during data cleaning
- **Background Processing**: Asynchronous job processing
- **Progress Tracking**: Detailed progress reporting at each pipeline stage

## Architecture

The backend uses a **LangGraph-based agent pipeline** with specialized agents:

```
Schema Validator → Missing Imputer → Outlier Detector → Transformer → Report Generator
```

### Pipeline Agents

1. **SchemaValidatorAgent**: Analyzes dataset schema, detects domain, identifies data types and units
2. **MissingImputerAgent**: Imputes missing values using appropriate strategies
3. **OutlierDetectorAgent**: Detects and handles outliers using multiple methods
4. **TransformerAgent**: Applies data transformations based on LLM recommendations
5. **ReportGeneratorAgent**: Generates comprehensive cleaning reports

## Requirements

- Python 3.11+
- HuggingFace API token (for LLM features)

## Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd data-cleaning-backend
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up environment variables**
   ```bash
   cp .env.example .env
   # Edit .env and add your HuggingFace token
   HF_TOKEN=your_huggingface_token_here
   ```

5. **Run the server**
   ```bash
   uvicorn app.main:app --reload
   ```

The API will be available at `http://localhost:8000`

## API Endpoints

### Upload CSV File
```http
POST /api/v1/upload
Content-Type: multipart/form-data

Body: CSV file
Response: { "job_id": "uuid", "status": "uploaded" }
```

### Start Processing
```http
POST /api/v1/process/{job_id}
Response: { "job_id": "uuid", "status": "processing_started" }
```

### Check Status
```http
GET /api/v1/status/{job_id}
Response: { "status": "processing|completed|failed", "progress": 0-100 }
```

### Download Cleaned CSV
```http
GET /api/v1/download/{job_id}/csv
Response: CSV file download
```

### Download Report
```http
GET /api/v1/download/{job_id}/report
Response: JSON report with cleaning details
```

### WebSocket (Real-time Updates)
```http
WS /api/v1/ws/{job_id}
```

## Response Format

### Cleaning Report Structure
```json
{
  "summary": "Data cleaning pipeline completed successfully.",
  "steps": {
    "schema": {
      "columns": [...],
      "dtypes": {...},
      "llm_analysis": "...",
      "statistics": "..."
    },
    "missing": {
      "missing_counts": {...},
      "imputed_columns": [...],
      "skipped_columns": [...]
    },
    "outliers": {
      "outliers_found": {...},
      "methods_used": {...},
      "actions_taken": {...}
    },
    "transformations": {
      "transformations": [...],
      "removed_columns": [...],
      "errors": [...]
    }
  }
}
```

## Technology Stack

- **FastAPI**: Modern, fast web framework
- **LangGraph**: Agent workflow orchestration
- **LangChain**: LLM integration and tools
- **Pandas**: Data manipulation and analysis
- **scikit-learn**: Machine learning utilities for imputation and outlier detection
- **WebSockets**: Real-time communication
- **HuggingFace**: LLM models for intelligent analysis

## 📁 Project Structure

```
data-cleaning-backend/
├── app/
│   ├── agents/          # Pipeline agents
│   │   ├── schema_validator.py
│   │   ├── missing_imputer.py
│   │   ├── outlier_detector.py
│   │   ├── transformer.py
│   │   └── report_generator.py
│   ├── api/             # API routes and WebSocket
│   │   ├── routes.py
│   │   └── websocket.py
│   ├── core/            # Core pipeline logic
│   │   ├── pipeline.py
│   │   ├── state.py
│   │   └── llm_manager.py
│   ├── utils/           # Utility functions
│   │   ├── transformations.py
│   │   ├── advanced_transformations.py
│   │   ├── llm_parser.py
│   │   └── file_handlers.py
│   ├── config.py
│   └── main.py
├── storage/
│   ├── uploads/         # Uploaded CSV files
│   ├── outputs/         # Cleaned CSV files
│   └── reports/         # Cleaning reports
├── requirements.txt
└── README.md
```

## Environment Variables

```env
APP_NAME=Data Cleaning Backend
DEBUG=True
API_V1_STR=/api/v1
HF_TOKEN=your_huggingface_token
UPLOAD_DIR=storage/uploads
OUTPUT_DIR=storage/outputs
REPORT_DIR=storage/reports
```

## Deployment

The backend is configured for deployment on Render. Key deployment steps:

1. Connect your GitHub repository to Render
2. Set environment variables in Render dashboard
3. Configure build command: `pip install -r requirements.txt`
4. Set start command: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`

## 📝 Development

### Running Tests
```bash
pytest tests/
```


## 🔗 Related Links

- [Frontend Repository](https://github.com/bhavanapuritipati/data-cleaning-frontend)
- [API Documentation](https://data-polisher.onrender.com/docs)
- [Live Frontend](https://data-polisher-ui.vercel.app/)
