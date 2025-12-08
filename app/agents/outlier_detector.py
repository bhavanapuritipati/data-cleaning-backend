from app.core.state import PipelineState
from app.agents.base_agent import BaseAgent
import numpy as np

class OutlierDetectorAgent(BaseAgent):
    def __init__(self):
        super().__init__("outlier_detector")

    async def process(self, state: PipelineState) -> PipelineState:
        self.update_progress(state, 50, "detecting outliers")
        df = state.get("current_df")
        
        outlier_report = {
            "outliers_found": {}
        }
        
        # IQR method for numeric columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)]
            if not outliers.empty:
                outlier_report["outliers_found"][col] = len(outliers)
                # Cap outliers for now
                df[col] = np.where(df[col] < lower_bound, lower_bound, df[col])
                df[col] = np.where(df[col] > upper_bound, upper_bound, df[col])

        state["current_df"] = df
        state["outlier_report"] = outlier_report
        self.update_progress(state, 60, "outlier detection complete")
        return state
