from app.core.state import PipelineState
from app.agents.base_agent import BaseAgent

class MissingImputerAgent(BaseAgent):
    def __init__(self):
        super().__init__("imputer")

    async def process(self, state: PipelineState) -> PipelineState:
        self.update_progress(state, 30, "imputing missing values")
        df = state.get("current_df")
        
        # Simplified Logic for now
        missing_report = {
            "missing_counts": df.isnull().sum().to_dict(),
            "imputed_columns": []
        }
        
        # Simple imputation: fill numeric with mean, object with mode
        for col in df.columns:
            if df[col].isnull().sum() > 0:
                if df[col].dtype in ['int64', 'float64']:
                    df[col] = df[col].fillna(df[col].mean())
                    missing_report["imputed_columns"].append({"col": col, "method": "mean"})
                else:
                    df[col] = df[col].fillna(df[col].mode()[0])
                    missing_report["imputed_columns"].append({"col": col, "method": "mode"})

        state["current_df"] = df
        state["imputation_report"] = missing_report
        self.update_progress(state, 40, "imputation complete")
        return state
