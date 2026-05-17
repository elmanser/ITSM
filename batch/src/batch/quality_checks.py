import pandas as pd
import json
from pathlib import Path
from src.batch.config import QUALITY_DIR
from src.common.logger import get_logger

logger = get_logger("batch.quality")

def run_quality_checks(df: pd.DataFrame, inc_rows: int, dem_rows: int):
    """Generates a data quality report."""
    logger.info("Running quality checks...")
    
    total_unified = len(df)
    
    report = {
        "total_rows_incidents": inc_rows,
        "total_rows_demandes": dem_rows,
        "total_rows_unified": total_unified,
        "missing_ticket_id": int(df["ticket_id"].isna().sum()),
        "missing_created_at": int(df["created_at"].isna().sum()),
        "missing_closed_at": int(df["closed_at"].isna().sum()),
        "missing_resolver_group": int(df["resolver_group"].isna().sum()),
        "missing_category_full": int(df["category_full"].isna().sum()),
        "missing_description": int(df["description"].isna().sum()),
        "negative_resolution_times": int((df["resolution_time_hours"] < 0).sum()),
        "closed_before_created": int((df["closed_at"] < df["created_at"]).sum()),
        "open_tickets": int(df["is_open"].sum()),
        "closed_tickets": int(df["is_closed"].sum()),
    }
    
    # Calculate duplicates
    dup_ids = df["ticket_id"].duplicated().sum()
    report["duplicate_ticket_ids"] = int(dup_ids)
    if dup_ids > 0:
        logger.warning(f"Found {dup_ids} duplicate ticket IDs in unified data!")
        
    # Percentage of nulls
    null_pct = (df.isna().sum() / total_unified * 100).round(2).to_dict()
    report["percentage_null_by_column"] = null_pct
    
    # Check for critical errors
    if report["negative_resolution_times"] > 0:
        logger.warning(f"Found {report['negative_resolution_times']} tickets with negative resolution times.")
        
    # Save report
    out_path = QUALITY_DIR / "quality_report.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=4, ensure_ascii=False)
        
    logger.info(f"Quality report saved to {out_path}")
    return report
