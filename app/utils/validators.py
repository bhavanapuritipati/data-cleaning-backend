import pandas as pd
from typing import List, Tuple

def validate_csv_content(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """
    Basic validation of CSV content.
    Returns (is_valid, list_of_errors).
    """
    errors = []
    
    if df.empty:
        errors.append("Dataset is empty")
        
    if len(df.columns) < 1:
        errors.append("Dataset has no columns")
        
    return len(errors) == 0, errors

def validate_file_extension(filename: str, allowed_extensions: List[str] = ['.csv']) -> bool:
    return any(filename.lower().endswith(ext) for ext in allowed_extensions)
