from app.core.state import PipelineState
from app.agents.base_agent import BaseAgent
from app.core.llm_manager import llm_manager
import json
import numpy as np
import pandas as pd

class ReportGeneratorAgent(BaseAgent):
    def __init__(self):
        super().__init__("reporter")

    async def process(self, state: PipelineState) -> PipelineState:
        self.update_progress(state, 90, "generating report")
        
        # Gather all reports and convert to JSON-serializable format
        final_report = {
            "summary": "Data cleaning pipeline completed successfully.",
            "steps": {
                "schema": self._make_json_serializable(state.get("schema_report")),
                "missing": self._make_json_serializable(state.get("imputation_report")),
                "outliers": self._make_json_serializable(state.get("outlier_report")),
                "transformations": self._make_json_serializable(state.get("transformation_report"))
            }
        }
        
        # Post-transformation validation with LLM (using sample for efficiency)
        transformation_report = state.get("transformation_report", {})
        if transformation_report.get("transformations"):
            validation_result = await self._validate_with_llm(
                state.get("current_df"),
                state.get("schema_report", {})
            )
            final_report["validation"] = self._make_json_serializable(validation_result)
        
        state["final_report"] = final_report
        self.update_progress(state, 100, "completed")
        return state
    
    async def _validate_with_llm(self, df, schema_report):
        """
        Validate transformations using LLM with sample data (5 rows).
        Efficient approach to confirm issues were resolved.
        """
        try:
            # Sample data for validation (max 5 rows)
            sample_size = min(5, len(df))
            sample_df = df.sample(n=sample_size, random_state=42)
            
            # Get original issues from schema analysis
            original_analysis = schema_report.get("llm_analysis", "")
            
            prompt = f"""
            Previously, you identified the following issues in this dataset:
            {original_analysis}
            
            After applying transformations, here is a sample of the cleaned data:
            {sample_df.to_string()}
            
            Please validate:
            1. Were the identified issues resolved?
            2. Is the data quality improved?
            3. Are there any new issues introduced?
            
            Provide a brief validation result in JSON format with keys: 'issues_resolved', 'quality_improved', 'new_issues', 'overall_assessment'.
            """
            
            llm_response = await llm_manager.generate_response(prompt)
            
            return {
                "llm_validation": llm_response,
                "sample_size": sample_size,
                "validation_method": "LLM-based sampling"
            }
            
        except Exception as e:
            return {
                "error": str(e),
                "sample_size": 0,
                "validation_method": "failed"
            }
    
    def _make_json_serializable(self, obj):
        """
        Convert pandas/numpy types to JSON-serializable Python types.
        """
        if obj is None:
            return None
        
        if isinstance(obj, dict):
            return {k: self._make_json_serializable(v) for k, v in obj.items()}
        
        if isinstance(obj, list):
            return [self._make_json_serializable(item) for item in obj]
        
        # Handle pandas/numpy numeric types
        if isinstance(obj, (np.integer, np.int64, np.int32)):
            return int(obj)
        
        if isinstance(obj, (np.floating, np.float64, np.float32)):
            return float(obj)
        
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        
        if isinstance(obj, pd.Series):
            return obj.tolist()
        
        # Handle NaN/Inf
        if isinstance(obj, float) and (np.isnan(obj) or np.isinf(obj)):
            return None
        
        return obj
