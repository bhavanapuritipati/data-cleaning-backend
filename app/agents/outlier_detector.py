from app.core.state import PipelineState
from app.agents.base_agent import BaseAgent
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from scipy import stats

class OutlierDetectorAgent(BaseAgent):
    def __init__(self):
        super().__init__("outlier_detector")

    async def process(self, state: PipelineState) -> PipelineState:
        self.update_progress(state, 50, "detecting outliers using multiple methods")
        df = state.get("current_df").copy()
        
        outlier_report = {
            "outliers_found": {},
            "methods_used": {},
            "actions_taken": {}
        }
        
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        if len(numeric_cols) == 0:
            state["current_df"] = df
            state["outlier_report"] = outlier_report
            self.update_progress(state, 60, "no numeric columns for outlier detection")
            return state
        
        print(f"[OutlierDetector] Analyzing {len(numeric_cols)} numeric columns")
        
        for col in numeric_cols:
            # Skip columns with too many missing values
            if df[col].isnull().mean() > 0.5:
                print(f"[OutlierDetector] Skipped {col}: too many missing values")
                continue
            
            # Check data distribution
            skewness = df[col].skew()
            is_highly_skewed = abs(skewness) > 1
            
            # Method 1: IQR (good for general cases)
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            lower_iqr = Q1 - 1.5 * IQR
            upper_iqr = Q3 + 1.5 * IQR
            
            iqr_outliers = df[(df[col] < lower_iqr) | (df[col] > upper_iqr)]
            iqr_count = len(iqr_outliers)
            
            # Method 2: Z-score (good for normal distributions)
            z_scores = np.abs(stats.zscore(df[col].dropna()))
            z_outliers_mask = z_scores > 3
            z_count = z_outliers_mask.sum()
            
            # Method 3: Isolation Forest (for complex patterns)
            # Only use if we have enough data points
            if len(df) >= 20:
                try:
                    iso_forest = IsolationForest(contamination=0.1, random_state=42)
                    iso_predictions = iso_forest.fit_predict(df[[col]].dropna())
                    iso_count = (iso_predictions == -1).sum()
                except:
                    iso_count = 0
            else:
                iso_count = 0
            
            # Decide which method to use and what action to take
            outlier_count = 0
            method_used = None
            action = "none"
            
            if is_highly_skewed:
                # For skewed data, use IQR and cap at percentiles
                if iqr_count > 0:
                    outlier_count = iqr_count
                    method_used = "IQR (skewed data)"
                    
                    # Cap at 1st and 99th percentiles (less aggressive)
                    p1 = df[col].quantile(0.01)
                    p99 = df[col].quantile(0.99)
                    original_vals = df[col].copy()
                    df[col] = np.where(df[col] < p1, p1, df[col])
                    df[col] = np.where(df[col] > p99, p99, df[col])
                    
                    action = "capped at 1st-99th percentile"
                    print(f"[OutlierDetector] ✓ {col}: {outlier_count} outliers capped (skewed)")
            
            else:
                # For normal-ish data, use consensus of methods
                # If 2+ methods agree, there are outliers
                methods_detecting = sum([iqr_count > 0, z_count > 0, iso_count > 0])
                
                if methods_detecting >= 2:
                    # Use the average count
                    outlier_count = int(np.mean([c for c in [iqr_count, z_count, iso_count] if c > 0]))
                    method_used = "consensus (IQR+Z-score+IsolForest)"
                    
                    # Cap using IQR bounds
                    original_vals = df[col].copy()
                    df[col] = np.where(df[col] < lower_iqr, lower_iqr, df[col])
                    df[col] = np.where(df[col] > upper_iqr, upper_iqr, df[col])
                    
                    action = "capped at IQR bounds"
                    print(f"[OutlierDetector] ✓ {col}: {outlier_count} outliers capped (consensus)")
            
            # Record findings
            if outlier_count > 0:
                outlier_report["outliers_found"][col] = outlier_count
                outlier_report["methods_used"][col] = method_used
                outlier_report["actions_taken"][col] = action
                outlier_report[f"{col}_details"] = {
                    "iqr_outliers": iqr_count,
                    "z_score_outliers": z_count,
                    "isolation_forest_outliers": iso_count,
                    "skewness": f"{skewness:.2f}",
                    "action": action
                }

        state["current_df"] = df
        state["outlier_report"] = outlier_report
        self.update_progress(state, 60, "outlier detection complete")
        return state
