import logging
import os
from logging.handlers import RotatingFileHandler

def get_logger(name: str) -> logging.Logger:
    """Returns a configured logger with standard formatting."""
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s — %(message)s", 
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        
        # Console handler
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        
        # File handler
        fh = RotatingFileHandler(
            os.path.join(log_dir, "batch_pipeline.log"),
            maxBytes=10*1024*1024, # 10 MB
            backupCount=5
        )
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        
    return logger
