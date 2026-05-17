import pandas as pd
import numpy as np
from src.batch.config import STATUS_CLOSED, STATUS_OPEN, LONG_RESOLUTION_HOURS
from src.common.logger import get_logger

logger = get_logger("batch.transformation")

def harmonize_incidents(df: pd.DataFrame) -> pd.DataFrame:
    """Harmonizes incidents into the unified schema."""
    out = pd.DataFrame()
    out["ticket_id"] = df["n_d_incident"]
    out["ticket_type"] = "Incident"
    out["created_at"] = pd.to_datetime(df["date_d_emission"], format="%d/%m/%Y %H:%M:%S", errors="coerce")
    out["closed_at"] = pd.to_datetime(df.get("date_de_fin"), format="%d/%m/%Y %H:%M:%S", errors="coerce")
    out["status"] = df.get("statut")
    out["priority"] = df.get("priorite")
    out["urgency"] = df.get("urgence")
    out["category_full"] = df.get("sujet_complet")
    out["service_label"] = np.nan
    out["sla"] = df.get("sla")
    out["location"] = df.get("localisation_dernier_niveau")
    out["entity"] = df.get("entite")
    out["description"] = df.get("description")
    out["resolver_group"] = df.get("resolu_par_groupe")
    out["resolver_agent"] = df.get("resolu_par_intervenant")
    out["origin"] = df.get("origine")
    out["updated_at"] = pd.to_datetime(df.get("derniere_mise_a_jour"), format="%d/%m/%Y %H:%M:%S", errors="coerce")
    return out

def harmonize_demandes(df: pd.DataFrame) -> pd.DataFrame:
    """Harmonizes demandes into the unified schema."""
    out = pd.DataFrame()
    out["ticket_id"] = df["n_de_demande"]
    out["ticket_type"] = "Demande"
    out["created_at"] = pd.to_datetime(df["date_d_emission"], format="%d/%m/%Y %H:%M:%S", errors="coerce")
    out["closed_at"] = pd.to_datetime(df.get("date_de_fin"), format="%d/%m/%Y %H:%M:%S", errors="coerce")
    out["status"] = df.get("statut")
    out["priority"] = np.nan
    out["urgency"] = np.nan
    out["category_full"] = df.get("libelle_complet")
    out["service_label"] = df.get("libelle_du_service")
    out["sla"] = np.nan
    out["location"] = df.get("localisation") if "localisation" in df else df.get("localisation_complete")
    out["entity"] = df.get("entite_du_beneficiaire") if "entite_du_beneficiaire" in df else df.get("entite_du_beneficiaire_complet")
    out["description"] = df.get("description")
    out["resolver_group"] = df.get("resolu_par_groupe")
    out["resolver_agent"] = df.get("resolu_par_intervenant")
    out["origin"] = np.nan
    out["updated_at"] = np.nan
    return out

def feature_engineering(df: pd.DataFrame) -> pd.DataFrame:
    """Adds calculated columns to the unified dataframe."""
    logger.info("Performing feature engineering...")
    df = df.copy()
    
    # Dates
    df["created_date"] = df["created_at"].dt.date
    df["created_year"] = df["created_at"].dt.year
    df["created_month"] = df["created_at"].dt.month
    df["created_week"] = df["created_at"].dt.isocalendar().week
    df["created_day"] = df["created_at"].dt.day
    df["created_day_of_week"] = df["created_at"].dt.dayofweek
    df["created_hour"] = df["created_at"].dt.hour
    df["is_weekend"] = df["created_day_of_week"] >= 5
    
    # Resolution
    df["resolution_time_hours"] = (df["closed_at"] - df["created_at"]).dt.total_seconds() / 3600.0
    df["resolution_time_days"] = df["resolution_time_hours"] / 24.0
    df["is_long_resolution"] = df["resolution_time_hours"] > LONG_RESOLUTION_HOURS
    
    # Status
    def normalize_status(val):
        if pd.isna(val): return np.nan
        val_lower = str(val).lower()
        if val_lower in STATUS_CLOSED: return "Closed"
        if val_lower in STATUS_OPEN: return "Open"
        return "Other"
        
    df["normalized_status"] = df["status"].apply(normalize_status)
    df["is_closed"] = df["normalized_status"] == "Closed"
    df["is_open"] = df["normalized_status"] == "Open"
    
    # Fix is_closed based on closed_at presence
    df.loc[df["closed_at"].notna(), "is_closed"] = True
    df.loc[df["closed_at"].notna(), "is_open"] = False
    
    # Description
    df["has_description"] = df["description"].notna()
    df["description_length"] = df["description"].fillna("").astype(str).str.len()
    
    # Categories
    # Assumes category_full format: "Incidents/Matériel/Informatiques"
    cat_split = df["category_full"].str.split("/", expand=True)
    df["category_level_1"] = cat_split[0] if 0 in cat_split.columns else np.nan
    df["category_level_2"] = cat_split[1] if 1 in cat_split.columns else np.nan
    df["category_level_3"] = cat_split[2] if 2 in cat_split.columns else np.nan
    df["category_level_4"] = cat_split[3] if 3 in cat_split.columns else np.nan
    
    # Support level
    def extract_level(grp):
        if pd.isna(grp): return np.nan
        grp_str = str(grp).upper()
        if "HD1" in grp_str: return "HD1"
        if "HD2" in grp_str: return "HD2"
        if "HD3" in grp_str: return "HD3"
        return "Other"
        
    df["support_level"] = df["resolver_group"].apply(extract_level)
    df["data_source"] = "csv_batch"
    
    return df

def build_dimensions(df: pd.DataFrame):
    """Extracts dimension tables."""
    # dim_category
    dim_category = df[["category_full", "category_level_1", "category_level_2", "category_level_3", "category_level_4"]].drop_duplicates().dropna(subset=["category_full"])
    dim_category["category_id"] = range(1, len(dim_category) + 1)
    
    # dim_group
    dim_group = df[["resolver_group", "support_level"]].drop_duplicates().dropna(subset=["resolver_group"])
    dim_group["group_id"] = range(1, len(dim_group) + 1)
    
    # dim_location
    dim_location = df[["location"]].drop_duplicates().dropna()
    dim_location["location_id"] = range(1, len(dim_location) + 1)
    
    # dim_entity
    dim_entity = df[["entity"]].drop_duplicates().dropna()
    dim_entity["entity_id"] = range(1, len(dim_entity) + 1)
    
    # dim_date
    min_date = df["created_date"].min()
    max_date = df["created_date"].max()
    if pd.isna(min_date):
        dim_date = pd.DataFrame()
    else:
        date_range = pd.date_range(start=min_date, end=max_date, freq="D")
        dim_date = pd.DataFrame({"date": date_range})
        dim_date["date_id"] = dim_date["date"].dt.strftime("%Y%m%d").astype(int)
        dim_date["day"] = dim_date["date"].dt.day
        dim_date["week"] = dim_date["date"].dt.isocalendar().week
        dim_date["month"] = dim_date["date"].dt.month
        dim_date["quarter"] = dim_date["date"].dt.quarter
        dim_date["year"] = dim_date["date"].dt.year
        dim_date["day_of_week"] = dim_date["date"].dt.dayofweek
        dim_date["is_weekend"] = dim_date["day_of_week"] >= 5
        dim_date["date"] = dim_date["date"].dt.date
        
    return {
        "dim_category": dim_category,
        "dim_group": dim_group,
        "dim_location": dim_location,
        "dim_entity": dim_entity,
        "dim_date": dim_date
    }
