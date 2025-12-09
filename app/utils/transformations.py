import pandas as pd
import re
from typing import Tuple, Optional, List
import numpy as np

class TransformationError(Exception):
    """Raised when a transformation fails validation"""
    pass


def clean_currency_column(df: pd.DataFrame, column: str, validate: bool = True) -> Tuple[pd.DataFrame, dict]:
    """
    Clean currency column by removing $, commas, and converting to float.
    
    Safety features:
    - Validates that >80% of values can be converted
    - Preserves original column as backup
    - Returns transformation report
    
    Returns: (modified_df, report)
    """
    if column not in df.columns:
        return df, {'success': False, 'error': f'Column {column} not found'}
    
    # Create backup
    backup_col = f"{column}_original"
    df[backup_col] = df[column].copy()
    
    try:
        # Clean the column
        cleaned = df[column].astype(str).str.replace('$', '', regex=False)
        cleaned = cleaned.str.replace(',', '', regex=False)
        cleaned = cleaned.str.strip()
        
        # Try conversion
        numeric_values = pd.to_numeric(cleaned, errors='coerce')
        
        # Validation: check conversion success rate
        if validate:
            success_rate = numeric_values.notna().sum() / len(numeric_values)
            if success_rate < 0.8:
                # Rollback
                df.drop(columns=[backup_col], inplace=True)
                raise TransformationError(
                    f"Only {success_rate:.1%} of values could be converted. "
                    f"Transformation aborted to prevent data loss."
                )
        
        # Apply transformation
        df[column] = numeric_values
        
        report = {
            'success': True,
            'column': column,
            'type': 'clean_currency',
            'rows_affected': numeric_values.notna().sum(),
            'conversion_rate': f"{numeric_values.notna().sum() / len(numeric_values):.1%}",
            'backup_column': backup_col
        }
        
        return df, report
        
    except Exception as e:
        # Rollback on any error
        if backup_col in df.columns:
            df.drop(columns=[backup_col], inplace=True)
        return df, {'success': False, 'error': str(e)}


def clean_year_column(df: pd.DataFrame, column: str, strategy: str = 'start_year') -> Tuple[pd.DataFrame, dict]:
    """
    Clean year column by extracting years from ranges like "2020-2021".
    
    Strategies:
    - 'start_year': Extract first year from range
    - 'end_year': Extract last year from range
    - 'keep_range': Keep as string (no transformation)
    
    Safety: Only transforms if >70% of values match year pattern
    """
    if column not in df.columns:
        return df, {'success': False, 'error': f'Column {column} not found'}
    
    # Create backup
    backup_col = f"{column}_original"
    df[backup_col] = df[column].copy()
    
    try:
        # Extract years using regex
        year_pattern = r'(\d{4})'
        
        def extract_year(value):
            if pd.isna(value):
                return np.nan
            matches = re.findall(year_pattern, str(value))
            if not matches:
                return np.nan
            if strategy == 'start_year':
                return int(matches[0])
            elif strategy == 'end_year':
                return int(matches[-1])
            else:
                return value
        
        if strategy != 'keep_range':
            cleaned = df[column].apply(extract_year)
            
            # Validation
            success_rate = cleaned.notna().sum() / len(cleaned)
            if success_rate < 0.7:
                df.drop(columns=[backup_col], inplace=True)
                raise TransformationError(
                    f"Only {success_rate:.1%} of values matched year pattern. "
                    f"Transformation aborted."
                )
            
            df[column] = cleaned
        
        report = {
            'success': True,
            'column': column,
            'type': 'clean_year',
            'strategy': strategy,
            'backup_column': backup_col
        }
        
        return df, report
        
    except Exception as e:
        if backup_col in df.columns:
            df.drop(columns=[backup_col], inplace=True)
        return df, {'success': False, 'error': str(e)}


def clean_text_column(df: pd.DataFrame, column: str) -> Tuple[pd.DataFrame, dict]:
    """
    Generic text cleaning: trim whitespace, normalize spacing.
    Low-risk transformation.
    """
    if column not in df.columns:
        return df, {'success': False, 'error': f'Column {column} not found'}
    
    try:
        df[column] = df[column].astype(str).str.strip()
        df[column] = df[column].str.replace(r'\s+', ' ', regex=True)
        
        report = {
            'success': True,
            'column': column,
            'type': 'clean_text'
        }
        
        return df, report
        
    except Exception as e:
        return df, {'success': False, 'error': str(e)}


def rollback_transformation(df: pd.DataFrame, report: dict) -> pd.DataFrame:
    """
    Rollback a transformation using the backup column.
    """
    if not report.get('success') or 'backup_column' not in report:
        return df
    
    column = report['column']
    backup_col = report['backup_column']
    
    if backup_col in df.columns:
        df[column] = df[backup_col].copy()
        df.drop(columns=[backup_col], inplace=True)
    
    return df
