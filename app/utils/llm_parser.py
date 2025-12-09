import json
import re
from typing import Dict, List, Any, Optional

def parse_llm_analysis(llm_text: str) -> Optional[Dict]:
    """
    Parse LLM response to extract JSON analysis.
    Handles multiple formats: plain JSON, markdown code blocks, mixed text.
    
    Returns None if parsing fails (safe fallback).
    """
    if not llm_text:
        return None
    
    try:
        # Try direct JSON parse first
        return json.loads(llm_text)
    except json.JSONDecodeError:
        pass
    
    # Try to extract JSON from markdown code blocks
    json_pattern = r'```(?:json)?\s*(\{.*?\})\s*```'
    matches = re.findall(json_pattern, llm_text, re.DOTALL)
    
    for match in matches:
        try:
            return json.loads(match)
        except json.JSONDecodeError:
            continue
    
    # Try to find any JSON object in the text
    json_obj_pattern = r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}'
    matches = re.findall(json_obj_pattern, llm_text, re.DOTALL)
    
    for match in matches:
        try:
            return json.loads(match)
        except json.JSONDecodeError:
            continue
    
    return None


def extract_transformation_tasks(analysis: Optional[Dict]) -> List[Dict[str, Any]]:
    """
    Extract actionable transformation tasks from LLM analysis.
    Generalized to work with various LLM response formats.
    
    Returns list of tasks with: {type, column, description, confidence}
    """
    if not analysis or not isinstance(analysis, dict):
        return []
    
    tasks = []
    
    # Look for common keys that indicate issues/suggestions
    issue_keys = ['potential_issues', 'issues', 'problems', 'suggestions', 'recommendations']
    
    for key in issue_keys:
        if key in analysis:
            issues = analysis[key]
            if isinstance(issues, list):
                for issue in issues:
                    # Pass the full analysis so we can check units field
                    task = _parse_issue_to_task(issue, analysis)
                    if task:
                        tasks.append(task)
    
    return tasks


def _parse_issue_to_task(issue: Any, analysis: Optional[Dict] = None) -> Optional[Dict[str, Any]]:
    """
    Convert an issue description to a transformation task.
    Uses keyword matching to identify transformation type.
    Also checks the 'units' field from analysis to improve accuracy.
    """
    if not isinstance(issue, dict):
        return None
    
    column = issue.get('column', '')
    description = issue.get('issue', '') or issue.get('description', '')
    
    if not column or not description:
        return None
    
    # Extract all column names from the text (handles "col1 and col2" or "col1, col2")
    # Look for quoted column names in the description
    import re
    quoted_cols = re.findall(r"'([^']+)'", description)
    if not quoted_cols:
        quoted_cols = re.findall(r'"([^"]+)"', description)
    
    # If we found quoted columns in description, use those instead
    if quoted_cols:
        # Filter to only actual column names (not full sentences)
        potential_cols = [c for c in quoted_cols if len(c) < 100]
        if potential_cols:
            column = ', '.join(potential_cols)
    
    description_lower = description.lower()
    
    # Check if the column has a unit specified (this is more reliable than description keywords)
    # Handle both single column and comma-separated columns
    columns_to_check = [c.strip() for c in column.split(',')]
    unit_for_column = None
    if analysis and isinstance(analysis, dict) and "units" in analysis:
        units_map = analysis["units"]
        for col_check in columns_to_check:
            # Try exact match first
            if col_check in units_map:
                unit_for_column = units_map[col_check]
                break
            # Try case-insensitive match
            for unit_col, unit_val in units_map.items():
                if unit_col.lower() == col_check.lower():
                    unit_for_column = unit_val
                    break
            if unit_for_column:
                break
    
    # Identify transformation type based on keywords AND units field
    task_type = 'unknown'
    confidence = 0.5  # Default medium confidence
    
    # PRIORITY: Check units field first (more reliable than description keywords)
    if unit_for_column:
        if unit_for_column in ["$", "₹", "€", "£", "¥"]:
            task_type = 'clean_currency'
            confidence = 0.9  # High confidence when unit is specified
        elif unit_for_column in ["%", "percent"]:
            task_type = 'clean_percentage'
            confidence = 0.9
    
    # Fallback to keyword matching if no unit found
    if task_type == 'unknown':
        # Currency/numeric cleaning
        if any(kw in description_lower for kw in ['comma', 'dollar', '$', 'currency', 'numeric', '₹', '€', '£', '¥']):
            task_type = 'clean_currency'
            confidence = 0.8
        
        # Date/year cleaning (but skip if we already identified currency via units)
        elif any(kw in description_lower for kw in ['year', 'date', 'time', 'range']):
            # Only classify as year if it's not a currency column
            # Check if description mentions year ranges like "2020-2021"
            if re.search(r'\d{4}[-–]\d{4}', description) or 'year' in description_lower:
                task_type = 'clean_year'
                confidence = 0.7
            else:
                # If "range" appears but no year pattern, might be numeric range (keep as unknown)
                task_type = 'unknown'
                confidence = 0.5
        
        # Percentage normalization
        elif any(kw in description_lower for kw in ['percent', '%', 'percentage']):
            task_type = 'clean_percentage'
            confidence = 0.8
        
        # Boolean conversion
        elif any(kw in description_lower for kw in ['yes/no', 'true/false', 'boolean', 'binary']):
            task_type = 'clean_boolean'
            confidence = 0.7
        
        # Text normalization
        elif any(kw in description_lower for kw in ['text', 'string', 'format', 'whitespace', 'inconsistent format']):
            task_type = 'clean_text'
            confidence = 0.6
        
        # Type conversion
        elif any(kw in description_lower for kw in ['type', 'dtype', 'convert', 'cast']):
            task_type = 'convert_type'
            confidence = 0.7
    
    return {
        'type': task_type,
        'column': column,
        'description': description,
        'confidence': confidence,
        'original_issue': issue,
        'unit_detected': unit_for_column  # For debugging
    }


def should_apply_transformation(task: Dict[str, Any], min_confidence: float = 0.6) -> bool:
    """
    Safety check: only apply transformation if confidence is high enough.
    This prevents mis-transformations from uncertain LLM suggestions.
    """
    return task.get('confidence', 0) >= min_confidence and task.get('type') != 'unknown'
