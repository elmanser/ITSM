import argparse
import pandas as pd
from pathlib import Path

from src.batch.config import (
    RAW_INCIDENTS_DIR, RAW_DEMANDES_DIR, PROCESSED_INCIDENTS_DIR, 
    PROCESSED_DEMANDES_DIR, CURATED_DIR, INCIDENTS_EXPECTED_COLUMNS, 
    DEMANDES_EXPECTED_COLUMNS
)
from src.batch.ingestion import load_csv
from src.batch.validation import inspect_schema
from src.batch.cleaning import clean_dataframe, normalize_column_names
from src.batch.transformation import harmonize_incidents, harmonize_demandes, feature_engineering, build_dimensions
from src.batch.quality_checks import run_quality_checks
from src.batch.kpi_calculation import calculate_kpis
from src.batch.ml_dataset_builder import build_ml_datasets
from src.common.logger import get_logger

logger = get_logger("batch.main")

def main():
    parser = argparse.ArgumentParser(description="ITSM Batch Data Engineering Pipeline")
    parser.add_argument("--incidents", type=str, help="Path to raw incidents CSV", 
                        default=str(RAW_INCIDENTS_DIR / "Tous les incidents - 2026-05-15T155729.790.csv"))
    parser.add_argument("--demandes", type=str, help="Path to raw demandes CSV",
                        default=str(RAW_DEMANDES_DIR / "Demandes - 2026-05-15T155945.563.csv"))
    args = parser.parse_args()

    logger.info("=== Starting Batch Pipeline ===")
    
    # 1. Ingestion
    logger.info("1. Ingestion")
    df_inc_raw = load_csv(args.incidents)
    df_dem_raw = load_csv(args.demandes)
    
    # 2. Validation
    logger.info("2. Schema Inspection")
    valid_inc, _ = inspect_schema(df_inc_raw, "Incidents", INCIDENTS_EXPECTED_COLUMNS)
    valid_dem, _ = inspect_schema(df_dem_raw, "Demandes", DEMANDES_EXPECTED_COLUMNS)
    if not valid_inc or not valid_dem:
        logger.error("Schema validation failed. Aborting pipeline.")
        return

    # 3. Cleaning
    logger.info("3. Cleaning")
    df_inc_clean = clean_dataframe(df_inc_raw, "N° d'Incident")
    df_dem_clean = clean_dataframe(df_dem_raw, "N° de demande")
    
    df_inc_clean = normalize_column_names(df_inc_clean)
    df_dem_clean = normalize_column_names(df_dem_clean)
    
    # Save processed (intermediate)
    df_inc_clean.to_csv(PROCESSED_INCIDENTS_DIR / "incidents_clean.csv", index=False)
    df_dem_clean.to_csv(PROCESSED_DEMANDES_DIR / "demandes_clean.csv", index=False)

    # 4. Harmonization
    logger.info("4. Harmonization")
    df_inc_harm = harmonize_incidents(df_inc_clean)
    df_dem_harm = harmonize_demandes(df_dem_clean)
    
    df_unified = pd.concat([df_inc_harm, df_dem_harm], ignore_index=True)
    
    # 5. Feature Engineering
    logger.info("5. Feature Engineering")
    fact_tickets = feature_engineering(df_unified)
    
    # 6. Build Dimensions
    logger.info("6. Building Analytical Dimensions")
    dims = build_dimensions(fact_tickets)
    
    # 7. Quality Checks
    logger.info("7. Data Quality Checks")
    run_quality_checks(fact_tickets, len(df_inc_harm), len(df_dem_harm))
    
    # 8. KPIs
    logger.info("8. Calculating KPIs")
    calculate_kpis(fact_tickets)
    
    # 9. ML Datasets
    logger.info("9. Preparing ML Datasets")
    build_ml_datasets(fact_tickets)
    
    # Save Curated Data
    logger.info("Saving Curated Data...")
    fact_out = CURATED_DIR / "fact_tickets.csv"
    fact_tickets.to_csv(fact_out, index=False)
    
    try:
        fact_tickets.to_parquet(CURATED_DIR / "fact_tickets.parquet", index=False)
    except ImportError:
        logger.warning("pyarrow/fastparquet not installed. Skipping parquet export.")

    for dim_name, dim_df in dims.items():
        if not dim_df.empty:
            dim_df.to_csv(CURATED_DIR / f"{dim_name}.csv", index=False)

    logger.info("=== Batch Pipeline Completed Successfully ===")

if __name__ == "__main__":
    main()
