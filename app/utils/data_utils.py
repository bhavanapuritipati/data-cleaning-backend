import pandas as pd
from typing import Dict, Any, List, Optional
import io

def get_dataframe_info(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Returns summary statistics and info about the dataframe.
    """
    buffer = io.StringIO()
    df.info(buf=buffer)
    info_str = buffer.getvalue()
    
    return {
        "columns": list(df.columns),
        "shape": df.shape,
        "missing_values": df.isnull().sum().to_dict(),
        "dtypes": df.dtypes.astype(str).to_dict(),
        "summary": df.describe().to_dict(),
        "info": info_str
    }

def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize column names: lowercase, strip, replace spaces with underscores.
    """
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace(r'[^\w]', '', regex=True)
    return df

def df_to_json_preview(df: pd.DataFrame, limit: int = 5) -> List[Dict[str, Any]]:
    """
    Convert first N rows to JSON for preview.
    """
    # Replace NaN with None for JSON serialization
    return df.head(limit).where(pd.notnull(df), None).to_dict(orient='records')
