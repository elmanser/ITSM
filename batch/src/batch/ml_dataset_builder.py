import pandas as pd
from src.batch.config import ML_DIR
from src.common.logger import get_logger

logger = get_logger("batch.ml")

def build_ml_datasets(df: pd.DataFrame):
    """Prepares and saves ML-ready datasets."""
    logger.info("Building ML datasets...")
    
    # Common features
    features = [
        "ticket_type", "priority", "urgency", "category_level_1", "category_level_2",
        "category_level_3", "location", "entity", "service_label", "origin",
        "created_hour", "created_day_of_week", "is_weekend", "description_length"
    ]
    
    # 1. Long Resolution Dataset
    # Target: is_long_resolution
    # Exclude open tickets and tickets with missing targets
    lr_df = df.dropna(subset=["is_long_resolution", "closed_at"]).copy()
    lr_dataset = lr_df[features + ["is_long_resolution"]]
    lr_out = ML_DIR / "ml_dataset_long_resolution.csv"
    lr_dataset.to_csv(lr_out, index=False)
    logger.info(f"Saved Long Resolution ML dataset: {len(lr_dataset)} rows.")
    
    # 2. Resolver Group Dataset
    # Target: resolver_group
    rg_df = df.dropna(subset=["resolver_group"]).copy()
    
    # Filter groups with >= 20 tickets
    group_counts = rg_df["resolver_group"].value_counts()
    valid_groups = group_counts[group_counts >= 20].index
    rg_dataset = rg_df[rg_df["resolver_group"].isin(valid_groups)][features + ["resolver_group"]]
    
    rg_out = ML_DIR / "ml_dataset_resolver_group.csv"
    rg_dataset.to_csv(rg_out, index=False)
    logger.info(f"Saved Resolver Group ML dataset: {len(rg_dataset)} rows.")
    
    # 3. Category Dataset
    # Target: category_level_2
    cat_df = df.dropna(subset=["category_level_2"]).copy()
    cat_dataset = cat_df[features + ["category_level_2"]]
    cat_out = ML_DIR / "ml_dataset_category.csv"
    cat_dataset.to_csv(cat_out, index=False)
    logger.info(f"Saved Category ML dataset: {len(cat_dataset)} rows.")
