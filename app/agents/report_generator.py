from app.core.state import PipelineState
from app.agents.base_agent import BaseAgent

class ReportGeneratorAgent(BaseAgent):
    def __init__(self):
        super().__init__("reporter")

    async def process(self, state: PipelineState) -> PipelineState:
        self.update_progress(state, 90, "generating report")
        
        final_report = {
            "summary": "Data cleaning pipeline completed successfully.",
            "steps": {
                "schema": state.get("schema_report"),
                "missing": state.get("imputation_report"),
                "outliers": state.get("outlier_report"),
                "transformations": state.get("transformation_report")
            }
        }
        
        state["final_report"] = final_report
        self.update_progress(state, 100, "completed")
        return state
