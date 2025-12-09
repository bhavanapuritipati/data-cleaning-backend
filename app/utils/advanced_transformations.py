import pandas as pd
import re
from typing import Tuple

def clean_percentage_column(df: pd.DataFrame, column: str) -> Tuple[pd.DataFrame, dict]:
    """
    Clean percentage column by converting "45%", "45 percent" → 0.45
    """
    if column not in df.columns:
        return df, {'success': False, 'error': f'Column {column} not found'}
    
    backup_col = f"{column}_original"
    df[backup_col] = df[column].copy()
    
    try:
        cleaned = df[column].astype(str).str.replace('%', '', regex=False)
        cleaned = cleaned.str.replace('percent', '', regex=False, case=False)
        cleaned = cleaned.str.strip()
        
        numeric_values = pd.to_numeric(cleaned, errors='coerce') / 100
        
        # Validation
        success_rate = numeric_values.notna().sum() / len(numeric_values)
        if success_rate < 0.7:
            df.drop(columns=[backup_col], inplace=True)
            return df, {'success': False, 'error': f'Only {success_rate:.1%} could be converted'}
        
        df[column] = numeric_values
        
        return df, {
            'success': True,
            'column': column,
            'type': 'clean_percentage',
            'conversion_rate': f"{success_rate:.1%}",
            'backup_column': backup_col
        }
    
    except Exception as e:
        if backup_col in df.columns:
            df.drop(columns=[backup_col], inplace=True)
        return df, {'success': False, 'error': str(e)}


def clean_boolean_column(df: pd.DataFrame, column: str) -> Tuple[pd.DataFrame, dict]:
    """
    Convert boolean-like strings to actual booleans
    "Yes"/"No", "True"/"False", "1"/"0" → True/False
    """
    if column not in df.columns:
        return df, {'success': False, 'error': f'Column {column} not found'}
    
    backup_col = f"{column}_original"
    df[backup_col] = df[column].copy()
    
    try:
        # Mapping
        true_vals = ['yes', 'true', '1', 't', 'y']
        false_vals = ['no', 'false', '0', 'f', 'n']
        
        cleaned = df[column].astype(str).str.lower().str.strip()
        boolean_series = cleaned.map(lambda x: True if x in true_vals else (False if x in false_vals else None))
        
        # Validation
        success_rate = boolean_series.notna().sum() / len(boolean_series)
        if success_rate < 0.7:
            df.drop(columns=[backup_col], inplace=True)
            return df, {'success': False, 'error': f'Only {success_rate:.1%} could be converted'}
        
        df[column] = boolean_series
        
        return df, {
            'success': True,
            'column': column,
            'type': 'clean_boolean',
            'conversion_rate': f"{success_rate:.1%}",
            'backup_column': backup_col
        }
    
    except Exception as e:
        if backup_col in df.columns:
            df.drop(columns=[backup_col], inplace=True)
        return df, {'success': False, 'error': str(e)}
