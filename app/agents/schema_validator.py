import pandas as pd
import numpy as np
from typing import Dict, Any
from app.core.state import PipelineState
from app.agents.base_agent import BaseAgent
from app.core.llm_manager import llm_manager

class SchemaValidatorAgent(BaseAgent):
    def __init__(self):
        super().__init__("schema_validator")

    async def process(self, state: PipelineState) -> PipelineState:
        """
        Validates the schema of the uploaded CSV with comprehensive statistics.
        """
        self.update_progress(state, 10, "analyzing schema and statistics")
        
        df = state.get("current_df")
        if df is None:
            state["errors"].append("No DataFrame found in state")
            state["status"] = "failed"
            return state

        # 1. Basic properties
        validation_report = {
            "columns": list(df.columns),
            "dtypes": df.dtypes.astype(str).to_dict(),
            "nrows": len(df),
            "ncols": len(df.columns),
            "issues": []
        }

        # 2. Check for empty columns
        empty_cols = [col for col in df.columns if df[col].isnull().all()]
        if empty_cols:
            validation_report["issues"].append(f"Empty columns detected: {empty_cols}")

        # 3. Generate comprehensive statistics for LLM
        stats_summary = self._generate_statistics(df)
        sample_data = df.head(10).to_string()  # Send 10 rows for better context
        
        # 4. Enhanced LLM prompt with statistics and removal rules
        prompt = f"""
        Analyze this dataset comprehensively. Make intelligent cleaning recommendations.
        
        DATASET OVERVIEW:
        - Rows: {len(df)}
        - Columns: {len(df.columns)}
        
        COLUMN STATISTICS:
        {stats_summary}
        
        SAMPLE DATA (first 10 rows):
        {sample_data}
        
        ANALYSIS REQUIRED:
        1. Domain: Identify the likely domain (e-commerce, healthcare, finance, entertainment, etc.)
        2. Data Types: Classify each column (numerical/categorical/datetime/text/ID/boolean)
        3. Units: Detect units in column names or values (%, $, ₹, €, kg, years, etc.)
        4. Column Removal: Follow these GENERALIZED RULES for remove_candidates:
           
           REMOVE columns if they meet ALL of these criteria:
           - Have >70% missing data AND are not useful/predictive (e.g., categorical columns with mostly missing values)
           - IDs with no predictive value (reference numbers, internal codes)
           - Zero variance (all same value)
           - Duplicate or highly correlated columns (same information as another column)
           
           DO NOT REMOVE columns even if they have high cardinality (>80% unique) if they are:
           - Year/Date/Time columns (e.g., "Year", "Year(s)", "Date", "Time", "Timestamp") - these are valuable temporal features
           - Important categorical identifiers that may naturally have high cardinality
           - Key business metrics or identifiers
           
           High cardinality alone is NOT a reason to remove a column. Only remove if the column has NO predictive value AND has other issues (high missingness, duplicate, etc.).
           
           For columns with high missingness (>70%):
           - If it's a categorical/text column with little information: REMOVE (add to remove_candidates)
           - If it's a year/date column: DO NOT REMOVE (keep, just note the missing data in potential_issues)
           - If it's a numerical column that could be imputed: DO NOT REMOVE (note in potential_issues)
           
        5. Data Issues: Identify problems (wrong data types, invalid ranges, inconsistent formats, missing data)
        6. Cleaning Priorities: What should be cleaned first?
        
        IMPORTANT REMOVAL GUIDELINES:
        - Year/Date columns: NEVER mark for removal due to high cardinality alone
        - High missingness (>70%) + No predictive value = REMOVE
        - High missingness (>70%) + Year/Date column = DO NOT REMOVE (fix missing data instead)
        - High cardinality + Useful information = DO NOT REMOVE
        - High cardinality + No information value + Other issues = Consider removal
        
        Provide analysis in JSON format:
        {{
          "domain": "...",
          "data_types": {{"column": "type", ...}},
          "units": {{"column": "unit", ...}},
          "remove_candidates": [
            {{"column": "...", "reason": "...", "confidence": 0.8}}
          ],
          "potential_issues": [
            {{"column": "...", "issue": "...", "severity": "high/medium/low"}}
          ],
          "cleaning_priorities": ["priority1", "priority2", ...]
        }}
        """
        
        try:
            llm_response = await llm_manager.generate_response(prompt)
            validation_report["llm_analysis"] = llm_response
            validation_report["statistics"] = stats_summary
        except Exception as e:
            validation_report["llm_analysis_error"] = str(e)

        # Update state
        state["schema_report"] = validation_report
        
        self.update_progress(state, 20, "schema validation complete")
        return state
    
    def _generate_statistics(self, df: pd.DataFrame) -> str:
        """
        Generate comprehensive statistics for each column.
        """
        stats_lines = []
        
        for col in df.columns:
            stats = {
                "name": col,
                "dtype": str(df[col].dtype),
                "missing": f"{df[col].isnull().sum()} ({df[col].isnull().mean()*100:.1f}%)",
                "unique": df[col].nunique(),
                "unique_pct": f"{df[col].nunique() / len(df) * 100:.1f}%"
            }
            
            # Add numeric statistics if applicable
            if df[col].dtype in ['int64', 'float64']:
                stats["mean"] = f"{df[col].mean():.2f}"
                stats["std"] = f"{df[col].std():.2f}"
                stats["min"] = f"{df[col].min():.2f}"
                stats["max"] = f"{df[col].max():.2f}"
                stats["skew"] = f"{df[col].skew():.2f}"
            
            # Add categorical statistics
            elif df[col].dtype == 'object':
                top_values = df[col].value_counts().head(3)
                stats["top_values"] = ", ".join([f"{v}({c})" for v, c in top_values.items()])
            
            stats_line = f"\n{col}:"
            for key, val in stats.items():
                if key != "name":
                    stats_line += f"\n  - {key}: {val}"
            stats_lines.append(stats_line)
        
        return "\n".join(stats_lines)
