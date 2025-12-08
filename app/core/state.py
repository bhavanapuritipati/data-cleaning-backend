from typing import TypedDict, List, Dict, Any, Optional
import pandas as pd

class PipelineState(TypedDict):
    """
    Represents the state of the data cleaning pipeline.
    """
    job_id: str
    original_df: Optional[pd.DataFrame]
    current_df: Optional[pd.DataFrame]
    
    # Reports from each agent
    schema_report: Dict[str, Any]
    imputation_report: Dict[str, Any]
    outlier_report: Dict[str, Any]
    transformation_report: Dict[str, Any]
    final_report: Dict[str, Any]
    
    # Execution metadata
    current_agent: str
    progress: int
    errors: List[str]
    status: str  # 'processing', 'completed', 'failed'
