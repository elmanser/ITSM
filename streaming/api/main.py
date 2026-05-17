"""
FastAPI prediction service — /predict, /health, /metadata, /retrain
Rate-limited (60 req/min per IP) · Prometheus metrics at /metrics
"""
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict

import joblib
import numpy as np
import pandas as pd
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from pydantic import BaseModel, Field, field_validator
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from prometheus_fastapi_instrumentator import Instrumentator

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
logger = logging.getLogger("itsm_api")

# ── Model loading ─────────────────────────────────────────────────────────────
MODEL_PATH = os.getenv("ML_MODEL_PATH", "/app/models/model.joblib")
if not os.path.exists(MODEL_PATH):
    raise RuntimeError(f"Model not found at {MODEL_PATH}. Run the ML trainer first.")

bundle = joblib.load(MODEL_PATH)
model         = bundle["model"]
label_encoder = bundle["label_encoder"]
feature_cols  = bundle["feature_cols"]
mttr_model    = bundle.get("mttr_model")  # optional — older bundles may lack it

# ── App + rate limiter ────────────────────────────────────────────────────────
limiter = Limiter(key_func=get_remote_address, default_limits=["200/minute"])
instrumentator = Instrumentator()

@asynccontextmanager
async def lifespan(app: FastAPI):
    instrumentator.expose(app, include_in_schema=False)
    yield

app = FastAPI(
    title="ITSM Ticket Priority Prediction API",
    version="2.0.0",
    description="Predicts ticket priority (Very High … Very Low) — rate limited, Prometheus-instrumented.",
    lifespan=lifespan,
)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
instrumentator.instrument(app)

# ── Schemas ───────────────────────────────────────────────────────────────────
class TicketPayload(BaseModel):
    urgency:     int = Field(..., ge=1, le=5)
    impact:      int = Field(..., ge=1, le=5)
    hour_of_day: int = Field(..., ge=0, le=23)
    day_of_week: int = Field(..., ge=0, le=6)
    month:       int = Field(..., ge=1, le=12)
    category_type: str = Field(...)

    @field_validator("category_type", mode="before")
    @classmethod
    def non_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("category_type must not be empty")
        return v.lower()


# ── Feature builder ───────────────────────────────────────────────────────────
def build_feature_vector(payload: TicketPayload) -> pd.DataFrame:
    u, i = payload.urgency, payload.impact
    data = {col: 0 for col in feature_cols}
    data.update({
        "urgency":            u,
        "impact":             i,
        "urgency_x_impact":   u * i,
        "urgency_sq":         u ** 2,
        "impact_sq":          i ** 2,
        "severity_score":     u + i,
        "hour_of_day":        payload.hour_of_day,
        "day_of_week":        payload.day_of_week,
        "month":              payload.month,
        "is_business_hours":  int(8 <= payload.hour_of_day <= 18),
        "is_weekend":         int(payload.day_of_week >= 5),
        "quarter":            (payload.month - 1) // 3 + 1,
    })
    cat_col = f"category_type_{payload.category_type}"
    if cat_col in data:
        data[cat_col] = 1
    return pd.DataFrame([data])


# ── Endpoints ─────────────────────────────────────────────────────────────────
@app.get("/health", tags=["health"])
def health_check():
    return {
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "model_version": bundle.get("trained_at", "unknown"),
    }


@app.post("/predict", tags=["prediction"])
@limiter.limit("60/minute")
def predict(request: Request, payload: TicketPayload):
    """Predict priority label — max 60 requests/minute per IP."""
    try:
        df    = build_feature_vector(payload)
        probs = model.predict_proba(df)[0]
        idx   = int(np.argmax(probs))
        label = str(label_encoder.inverse_transform([idx])[0])
        result: Dict[str, Any] = {
            "predicted_label": label,
            "predicted_index": idx,
            "confidence": round(float(probs[idx]), 4),
            "probabilities": {
                str(label_encoder.inverse_transform([i])[0]): round(float(p), 4)
                for i, p in enumerate(probs)
            },
        }
        if mttr_model is not None:
            predicted_mttr = float(mttr_model.predict(df)[0])
            result["predicted_mttr_hours"] = round(max(0.0, predicted_mttr), 2)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/metadata", tags=["debug"])
def model_metadata():
    return {
        "trained_at": bundle.get("trained_at"),
        "algorithm":  bundle["metrics"]["algorithm"],
        "features":   feature_cols,
        "metrics":    bundle["metrics"],
    }


@app.post("/retrain", tags=["admin"])
def trigger_retrain(background_tasks: BackgroundTasks):
    """Trigger ML model retraining in background (non-blocking)."""
    from retrain import run_retraining  # lazy import — avoids heavy deps at startup
    background_tasks.add_task(run_retraining)
    logger.info("Retraining triggered via API")
    return {"status": "retraining_started", "timestamp": datetime.now(timezone.utc).isoformat()}
