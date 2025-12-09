from app.core.state import PipelineState
from app.agents.base_agent import BaseAgent
# Import experimental first, then the imputers
from sklearn.experimental import enable_iterative_imputer  # noqa
from sklearn.impute import SimpleImputer, KNNImputer, IterativeImputer
import pandas as pd
import numpy as np

class MissingImputerAgent(BaseAgent):
    def __init__(self):
        super().__init__("imputer")

    async def process(self, state: PipelineState) -> PipelineState:
        self.update_progress(state, 30, "analyzing missing values")
        df = state.get("current_df").copy()
        
        missing_report = {
            "missing_counts": df.isnull().sum().to_dict(),
            "imputed_columns": [],
            "skipped_columns": []
        }
        
        # Identify columns with missing values
        cols_with_missing = df.columns[df.isnull().any()].tolist()
        
        if not cols_with_missing:
            state["current_df"] = df
            state["imputation_report"] = missing_report
            self.update_progress(state, 40, "no missing values found")
            return state
        
        print(f"[MissingImputer] Found {len(cols_with_missing)} columns with missing values")
        
        # Process each column with smart strategy selection
        for col in cols_with_missing:
            missing_pct = df[col].isnull().mean() * 100
            
            # Rule 1: >70% missing → skip (suggest removal in report)
            if missing_pct > 70:
                missing_report["skipped_columns"].append({
                    "col": col,
                    "missing_pct": f"{missing_pct:.1f}%",
                    "reason": "Too much missing data (>70%), consider removing column"
                })
                print(f"[MissingImputer] Skipped {col}: {missing_pct:.1f}% missing")
                continue
            
            # Rule 2: Categorical columns
            if df[col].dtype == 'object':
                if missing_pct < 40:
                    # Low-moderate missing: use mode or "Unknown"
                    if df[col].mode().size > 0:
                        df[col] = df[col].fillna(df[col].mode()[0])
                        missing_report["imputed_columns"].append({
                            "col": col,
                            "method": "mode",
                            "missing_pct": f"{missing_pct:.1f}%"
                        })
                        print(f"[MissingImputer] ✓ {col}: mode imputation")
                    else:
                        df[col] = df[col].fillna("Unknown")
                        missing_report["imputed_columns"].append({
                            "col": col,
                            "method": "Unknown category",
                            "missing_pct": f"{missing_pct:.1f}%"
                        })
                        print(f"[MissingImputer] ✓ {col}: Unknown category")
                else:
                    missing_report["skipped_columns"].append({
                        "col": col,
                        "missing_pct": f"{missing_pct:.1f}%",
                        "reason": "High missingness in categorical column"
                    })
                    print(f"[MissingImputer] Skipped {col}: too many missing categorical values")
            
            # Rule 3: Numerical columns
            elif df[col].dtype in ['int64', 'float64']:
                try:
                    # Strategy selection based on missing percentage
                    if missing_pct < 5:
                        # Low missing: use median (robust to outliers)
                        median_val = df[col].median()
                        df[col] = df[col].fillna(median_val)
                        missing_report["imputed_columns"].append({
                            "col": col,
                            "method": "median",
                            "missing_pct": f"{missing_pct:.1f}%",
                            "value": f"{median_val:.2f}"
                        })
                        print(f"[MissingImputer] ✓ {col}: median imputation")
                    
                    elif missing_pct < 30:
                        # Moderate missing: use KNN imputation
                        # Create a temporary dataset with only numeric columns for KNN
                        numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
                        df_numeric = df[numeric_cols].copy()
                        
                        # Apply KNN imputation
                        n_neighbors = min(5, len(df) - 1)  # Ensure we don't exceed available rows
                        imputer = KNNImputer(n_neighbors=n_neighbors)
                        df_numeric_imputed = imputer.fit_transform(df_numeric)
                        
                        # Update only the specific column
                        col_idx = list(numeric_cols).index(col)
                        df[col] = df_numeric_imputed[:, col_idx]
                        
                        missing_report["imputed_columns"].append({
                            "col": col,
                            "method": f"KNN (k={n_neighbors})",
                            "missing_pct": f"{missing_pct:.1f}%"
                        })
                        print(f"[MissingImputer] ✓ {col}: KNN imputation")
                    
                    else:
                        # High missing (30-70%): use iterative imputation
                        numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
                        df_numeric = df[numeric_cols].copy()
                        
                        imputer = IterativeImputer(max_iter=10, random_state=42)
                        df_numeric_imputed = imputer.fit_transform(df_numeric)
                        
                        col_idx = list(numeric_cols).index(col)
                        df[col] = df_numeric_imputed[:, col_idx]
                        
                        missing_report["imputed_columns"].append({
                            "col": col,
                            "method": "iterative",
                            "missing_pct": f"{missing_pct:.1f}%"
                        })
                        print(f"[MissingImputer] ✓ {col}: iterative imputation")
                
                except Exception as e:
                    # Fallback to median if advanced methods fail
                    print(f"[MissingImputer] ⚠ Advanced imputation failed for {col}, using median: {e}")
                    median_val = df[col].median()
                    df[col] = df[col].fillna(median_val)
                    missing_report["imputed_columns"].append({
                        "col": col,
                        "method": "median (fallback)",
                        "missing_pct": f"{missing_pct:.1f}%",
                        "error": str(e)
                    })

        state["current_df"] = df
        state["imputation_report"] = missing_report
        self.update_progress(state, 40, "imputation complete")
        return state
