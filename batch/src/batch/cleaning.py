import pandas as pd
import numpy as np
import re
from typing import List
from src.common.logger import get_logger

logger = get_logger("batch.cleaning")

def clean_dataframe(df: pd.DataFrame, ticket_id_col: str) -> pd.DataFrame:
    """Cleans the dataframe according to specifications."""
    df_clean = df.copy()
    initial_rows = len(df_clean)
    
    # 1. Remove fully empty columns
    empty_cols = [c for c in df_clean.columns if df_clean[c].isna().all() or df_clean[c].eq("").all()]
    df_clean.drop(columns=empty_cols, inplace=True)
    
    # 2. Remove unnamed columns
    unnamed_cols = [c for c in df_clean.columns if "unnamed" in str(c).lower()]
    df_clean.drop(columns=unnamed_cols, inplace=True)
    
    # 3. Strip spaces from column names
    df_clean.columns = [str(c).strip() for c in df_clean.columns]
    
    # 4. Strip spaces from string values and normalize empty values
    null_values = ["", " ", "nan", "none", "null"]
    
    def normalize_val(val):
        if pd.isna(val):
            return np.nan
        if isinstance(val, str):
            v_strip = val.strip()
            if v_strip.lower() in null_values:
                return np.nan
            return v_strip
        return val

    # Apply element-wise
    df_clean = df_clean.map(normalize_val)
    
    # 5. Remove duplicate tickets
    df_clean.drop_duplicates(subset=[ticket_id_col], keep="last", inplace=True)
    final_rows = len(df_clean)
    if initial_rows - final_rows > 0:
        logger.info(f"Removed {initial_rows - final_rows} duplicate tickets based on {ticket_id_col}.")
        
    return df_clean

def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Converts column names to snake_case."""
    def to_snake_case(name):
        name = name.lower()
        name = name.replace("é", "e").replace("è", "e").replace("ê", "e").replace("à", "a")
        name = name.replace("°", "")
        name = name.replace("d'", "d_")
        name = name.replace("(", "").replace(")", "")
        name = re.sub(r'[^a-z0-9]+', '_', name)
        name = name.strip('_')
        return name
        
    df.rename(columns={c: to_snake_case(c) for c in df.columns}, inplace=True)
    return df
