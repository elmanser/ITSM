import pandas as pd
from pathlib import Path
from src.common.logger import get_logger

logger = get_logger("batch.ingestion")

def load_csv(file_path: str | Path, sep: str = ";") -> pd.DataFrame:
    """Reads a CSV file with robust encoding handling."""
    path = Path(file_path)
    if not path.exists():
        logger.error(f"File not found: {path}")
        raise FileNotFoundError(f"File not found: {path}")

    encodings = ["utf-8", "utf-8-sig", "latin1", "cp1252"]
    
    for enc in encodings:
        try:
            df = pd.read_csv(path, sep=sep, encoding=enc, dtype=str, index_col=False)
            logger.info(f"Successfully loaded '{path.name}' using encoding '{enc}'.")
            logger.info(f"Shape: {df.shape[0]} rows, {df.shape[1]} columns.")
            return df
        except UnicodeDecodeError:
            logger.debug(f"Failed to decode '{path.name}' with {enc}.")
        except Exception as e:
            logger.error(f"Error reading '{path.name}': {e}")
            raise
            
    logger.error(f"Failed to load '{path.name}' with all attempted encodings.")
    raise ValueError(f"Could not read {path.name}")
