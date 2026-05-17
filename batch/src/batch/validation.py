import pandas as pd
from typing import List, Tuple
from src.common.logger import get_logger

logger = get_logger("batch.validation")

def inspect_schema(df: pd.DataFrame, file_name: str, expected_cols: List[str]) -> Tuple[bool, List[str]]:
    """Inspects the schema and returns (is_valid, warnings)."""
    warnings = []
    logger.info(f"--- Schema Inspection: {file_name} ---")
    
    # 1. Detect empty columns
    empty_cols = [c for c in df.columns if df[c].isna().all() or df[c].eq("").all()]
    if empty_cols:
        msg = f"Detected completely empty columns: {empty_cols}"
        logger.warning(msg)
        warnings.append(msg)
        
    # 2. Detect duplicated columns
    dup_cols = df.columns[df.columns.duplicated()].tolist()
    if dup_cols:
        msg = f"Detected duplicated columns: {dup_cols}"
        logger.warning(msg)
        warnings.append(msg)
        
    # 3. Detect extra unnamed columns (usually from trailing delimiters)
    unnamed = [c for c in df.columns if "unnamed" in str(c).lower()]
    if unnamed:
        msg = f"Detected unnamed columns: {unnamed}"
        logger.warning(msg)
        warnings.append(msg)
        
    # 4. Check for missing expected columns
    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        msg = f"Missing expected columns: {missing}"
        logger.warning(msg)
        warnings.append(msg)
        
    # 5. Check for critical columns
    # At least we need a ticket ID and creation date
    # In both incidents and demandes, the creation date is "Date d'émission"
    # ID is either "N° d'Incident" or "N° de demande"
    critical_present = False
    if "Date d'émission" in df.columns and ("N° d'Incident" in df.columns or "N° de demande" in df.columns):
        critical_present = True
        
    if not critical_present:
        logger.error(f"Critical columns (Date d'émission and ID) are missing in {file_name}!")
        return False, warnings
        
    logger.info(f"Schema inspection passed for {file_name}. Columns: {list(df.columns)}")
    return True, warnings
