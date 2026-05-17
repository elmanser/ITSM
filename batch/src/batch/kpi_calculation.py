import pandas as pd
import json
from pathlib import Path
from src.batch.config import KPI_DIR
from src.common.logger import get_logger

logger = get_logger("batch.kpi")

def calculate_kpis(df: pd.DataFrame):
    """Calculates summary KPIs and saves them."""
    logger.info("Calculating KPIs...")
    
    kpis = {}
    kpis["total_tickets"] = int(len(df))
    kpis["total_incidents"] = int((df["ticket_type"] == "Incident").sum())
    kpis["total_demandes"] = int((df["ticket_type"] == "Demande").sum())
    kpis["open_tickets"] = int(df["is_open"].sum())
    kpis["closed_tickets"] = int(df["is_closed"].sum())
    kpis["tickets_without_closed_date"] = int(df["closed_at"].isna().sum())
    
    res_time = df.loc[df["resolution_time_hours"] >= 0, "resolution_time_hours"]
    kpis["average_resolution_time_hours"] = float(res_time.mean()) if not res_time.empty else 0.0
    kpis["median_resolution_time_hours"] = float(res_time.median()) if not res_time.empty else 0.0
    
    kpis["long_resolution_rate"] = float(df["is_long_resolution"].mean() * 100) if len(df) > 0 else 0.0
    
    # Distributions
    kpis["tickets_by_type"] = df["ticket_type"].value_counts().to_dict()
    kpis["tickets_by_status"] = df["normalized_status"].value_counts().to_dict()
    kpis["tickets_by_priority"] = df["priority"].value_counts().to_dict()
    kpis["tickets_by_urgency"] = df["urgency"].value_counts().to_dict()
    
    # Top 10s
    kpis["tickets_by_category_top10"] = df["category_level_2"].value_counts().head(10).to_dict()
    kpis["tickets_by_resolver_group_top10"] = df["resolver_group"].value_counts().head(10).to_dict()
    kpis["tickets_by_location_top10"] = df["location"].value_counts().head(10).to_dict()
    kpis["tickets_by_entity_top10"] = df["entity"].value_counts().head(10).to_dict()
    
    # Save to JSON
    json_path = KPI_DIR / "kpis_summary.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(kpis, f, indent=4, ensure_ascii=False)
        
    logger.info(f"KPIs saved to {json_path}")
    return kpis
