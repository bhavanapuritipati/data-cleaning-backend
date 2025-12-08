import pandas as pd
from typing import Dict, Any
from app.core.state import PipelineState
from app.agents.base_agent import BaseAgent
from app.core.llm_manager import llm_manager

class SchemaValidatorAgent(BaseAgent):
    def __init__(self):
        super().__init__("schema_validator")

    async def process(self, state: PipelineState) -> PipelineState:
        """
        Validates the schema of the uploaded CSV.
        """
        self.update_progress(state, 10, "validating schema")
        
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

        # 3. LLM-based Schema Analysis
        # We'll ask the LLM to infer the likely type of dataset and semantic meaning of columns
        schema_summary = f"Columns: {', '.join(df.columns)}. Sample data: {df.head(2).to_string()}"
        prompt = f"""
        Analyze the following dataset schema and sample data. 
        Identify the likely domain (e.g., e-commerce, healthcare) and any potential schema issues (e.g., 'age' column being text instead of number).
        
        Dataset Info:
        {schema_summary}
        
        Provide a brief analysis in JSON format with keys: 'domain', 'potential_issues'.
        """
        
        try:
            llm_response = await llm_manager.generate_response(prompt)
            validation_report["llm_analysis"] = llm_response
        except Exception as e:
            validation_report["llm_analysis_error"] = str(e)

        # Update state
        state["schema_report"] = validation_report
        
        # Determine if we should fail or proceed
        # For now, we proceed unless critical error
        
        self.update_progress(state, 20, "schema validation complete")
        return state
