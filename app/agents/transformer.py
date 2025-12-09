from app.core.state import PipelineState
from app.agents.base_agent import BaseAgent
from app.utils.llm_parser import parse_llm_analysis, extract_transformation_tasks, should_apply_transformation
from app.utils.transformations import (
    clean_currency_column, 
    clean_year_column, 
    clean_text_column,
    rollback_transformation
)
from app.utils.advanced_transformations import clean_percentage_column, clean_boolean_column

class TransformerAgent(BaseAgent):
    def __init__(self):
        super().__init__("transformer")

    async def process(self, state: PipelineState) -> PipelineState:
        self.update_progress(state, 70, "analyzing transformation needs")
        df = state.get("current_df").copy()  # Work on a copy for safety
        
        transformation_report = {
            "transformations": [],
            "skipped": [],
            "errors": []
        }
        
        # Parse LLM analysis from schema validation
        schema_report = state.get("schema_report", {})
        llm_analysis_text = schema_report.get("llm_analysis", "")
        
        # Extract transformation tasks from potential_issues
        llm_data = parse_llm_analysis(llm_analysis_text)
        tasks = extract_transformation_tasks(llm_data)
        
        # ENHANCEMENT: Also check the 'units' field from LLM analysis
        # If LLM identified units like "$", "%", etc., create transformation tasks automatically
        if llm_data and "units" in llm_data:
            import re
            units_map = llm_data["units"]
            for column, unit in units_map.items():
                # Check if we already have a task for this column
                existing_task = any(column in t.get("column", "") for t in tasks)
                if not existing_task:
                    # IMPORTANT: Fuzzy match the column name to DataFrame columns
                    # to handle non-breaking spaces and other whitespace issues
                    matched_df_col = None
                    col_clean = re.sub(r'\s+', ' ', column.strip('"').strip("'").strip())
                    
                    # First try exact match
                    if col_clean in df.columns:
                        matched_df_col = col_clean
                    else:
                        # Try fuzzy match
                        for df_col in df.columns:
                            df_col_clean = re.sub(r'\s+', ' ', df_col)
                            if df_col_clean.lower() == col_clean.lower():
                                matched_df_col = df_col
                                break
                    
                    if matched_df_col:
                        # Create task based on unit type using the matched DataFrame column name
                        if unit in ["$", "₹", "€", "£", "¥"]:
                            tasks.append({
                                "type": "clean_currency",
                                "column": matched_df_col,  # Use DataFrame column name, not LLM's
                                "description": f"Currency column with {unit} symbol",
                                "confidence": 0.9,
                                "source": "units_field"
                            })
                            print(f"[TransformerAgent] Added currency task for {matched_df_col} from units field")
                        elif unit in ["%", "percent"]:
                            tasks.append({
                                "type": "clean_percentage",
                                "column": matched_df_col,
                                "description": f"Percentage column with {unit}",
                                "confidence": 0.9,
                                "source": "units_field"
                            })
                            print(f"[TransformerAgent] Added percentage task for {matched_df_col} from units field")
                    else:
                        print(f"[TransformerAgent] ⚠ Could not match LLM column '{column}' to DataFrame")
        
        print(f"[TransformerAgent] Found {len(tasks)} potential transformations")
        
        # Apply transformations with safety checks
        for task in tasks:
            # Safety check: only apply if confidence is high enough
            if not should_apply_transformation(task, min_confidence=0.6):
                transformation_report["skipped"].append({
                    "column": task.get("column"),
                    "reason": f"Low confidence ({task.get('confidence', 0):.2f})",
                    "type": task.get("type")
                })
                print(f"[TransformerAgent] Skipped {task.get('column')} - low confidence")

                continue
            
            column = task.get("column")
            task_type = task.get("type")
            
            # Handle multi-column references (e.g., "Actual gross, Adjusted gross")
            columns = [c.strip() for c in column.split(',')]
            
            for col in columns:
                # Try exact match first
                if col in df.columns:
                    matched_col = col
                # Try fuzzy match (case-insensitive, strip quotes, normalize whitespace)
                else:
                    import re
                    # Normalize: strip quotes, normalize whitespace (replace \xa0, \t, multiple spaces with single space)
                    col_clean = col.strip('"').strip("'").strip()
                    col_clean = re.sub(r'\s+', ' ', col_clean)  # Replace any whitespace with single space
                    
                    matched_col = None
                    for df_col in df.columns:
                        df_col_clean = re.sub(r'\s+', ' ', df_col)  # Normalize DataFrame column too
                        if df_col_clean.lower() == col_clean.lower():
                            matched_col = df_col
                            break
                
                if not matched_col:
                    print(f"[TransformerAgent] ⚠ Column '{col}' not found in DataFrame")
                    print(f"[TransformerAgent]   Available columns: {list(df.columns)}")
                    continue
                
                try:
                    # Apply appropriate transformation
                    if task_type == "clean_currency":
                        df, report = clean_currency_column(df, matched_col, validate=True)
                    elif task_type == "clean_year":
                        df, report = clean_year_column(df, matched_col, strategy='start_year')
                    elif task_type == "clean_percentage":
                        df, report = clean_percentage_column(df, matched_col)
                    elif task_type == "clean_boolean":
                        df, report = clean_boolean_column(df, matched_col)
                    elif task_type == "clean_text":
                        df, report = clean_text_column(df, matched_col)
                    else:
                        report = {"success": False, "error": f"Unknown task type: {task_type}"}
                    
                    # Log result
                    if report.get("success"):
                        transformation_report["transformations"].append(report)
                        print(f"[TransformerAgent] ✓ Applied {task_type} to {matched_col}")
                    else:
                        transformation_report["errors"].append({
                            "column": matched_col,
                            "type": task_type,
                            "error": report.get("error", "Unknown error")
                        })
                        print(f"[TransformerAgent] ✗ Failed {task_type} on {matched_col}: {report.get('error')}")
                
                except Exception as e:
                    transformation_report["errors"].append({
                        "column": matched_col,
                        "type": task_type,
                        "error": str(e)
                    })
                    print(f"[TransformerAgent] ✗ Exception on {matched_col}: {e}")
        
        # Handle column removal based on remove_candidates from LLM analysis
        if llm_data and "remove_candidates" in llm_data:
            import re
            remove_candidates = llm_data["remove_candidates"]
            removed_columns = []
            
            for candidate in remove_candidates:
                if isinstance(candidate, dict):
                    column_name = candidate.get("column", "")
                    confidence = candidate.get("confidence", 0.5)
                    
                    # Only remove if confidence is high enough (>= 0.7)
                    if confidence >= 0.7:
                        # Try to match column name
                        matched_col = None
                        col_clean = re.sub(r'\s+', ' ', column_name.strip('"').strip("'").strip())
                        
                        # First try exact match
                        if col_clean in df.columns:
                            matched_col = col_clean
                        else:
                            # Try fuzzy match
                            for df_col in df.columns:
                                df_col_clean = re.sub(r'\s+', ' ', df_col)
                                if df_col_clean.lower() == col_clean.lower():
                                    matched_col = df_col
                                    break
                        
                        if matched_col and matched_col in df.columns:
                            df.drop(columns=[matched_col], inplace=True)
                            removed_columns.append({
                                "column": matched_col,
                                "reason": candidate.get("reason", "Marked for removal by LLM"),
                                "confidence": confidence
                            })
                            print(f"[TransformerAgent] ✓ Removed column '{matched_col}' (confidence: {confidence})")
            
            if removed_columns:
                transformation_report["removed_columns"] = removed_columns
        
        # Remove backup columns (_original) from final output
        backup_columns = [col for col in df.columns if col.endswith('_original')]
        if backup_columns:
            df.drop(columns=backup_columns, inplace=True)
            print(f"[TransformerAgent] ✓ Removed {len(backup_columns)} backup columns from output")
        
        # Update state with transformed data
        state["current_df"] = df
        state["transformation_report"] = transformation_report
        
        self.update_progress(state, 80, "transformation complete")
        return state
