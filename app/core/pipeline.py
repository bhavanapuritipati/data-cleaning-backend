from langgraph.graph import StateGraph, END
from app.core.state import PipelineState
from app.agents.schema_validator import SchemaValidatorAgent
from app.agents.missing_imputer import MissingImputerAgent
from app.agents.outlier_detector import OutlierDetectorAgent
from app.agents.transformer import TransformerAgent
from app.agents.report_generator import ReportGeneratorAgent

# Initialize agents
schema_validator = SchemaValidatorAgent()
imputer = MissingImputerAgent()
outlier_detector = OutlierDetectorAgent()
transformer = TransformerAgent()
reporter = ReportGeneratorAgent()

def create_pipeline():
    """
    Constructs the LangGraph pipeline for data cleaning.
    """
    workflow = StateGraph(PipelineState)
    
    # Define nodes
    workflow.add_node("schema_validator", schema_validator.process)
    workflow.add_node("imputer", imputer.process)
    workflow.add_node("outlier_detector", outlier_detector.process)
    workflow.add_node("transformer", transformer.process)
    workflow.add_node("reporter", reporter.process)
    
    # Define edges
    # Standard linear flow as per architecture
    workflow.add_edge("schema_validator", "imputer")
    workflow.add_edge("imputer", "outlier_detector")
    workflow.add_edge("outlier_detector", "transformer")
    workflow.add_edge("transformer", "reporter")
    workflow.add_edge("reporter", END)
    
    # Set entry point
    workflow.set_entry_point("schema_validator")
    
    # Compile
    app = workflow.compile()
    return app

pipeline = create_pipeline()
