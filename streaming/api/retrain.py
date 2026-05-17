"""Background ML retraining v2 — feature engineering + cross-validation."""
import logging
import os
from datetime import datetime

import joblib
import numpy as np
import pandas as pd
import psycopg2
from sklearn.ensemble import GradientBoostingClassifier, GradientBoostingRegressor, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, balanced_accuracy_score, f1_score, mean_absolute_error
from sklearn.model_selection import StratifiedKFold, cross_val_score, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import LabelEncoder, StandardScaler
from xgboost import XGBClassifier

logger = logging.getLogger("retrain")

PG_CONN = dict(
    host=os.getenv("POSTGRES_HOST", "postgres"),
    port=int(os.getenv("POSTGRES_PORT", "5432")),
    dbname=os.getenv("POSTGRES_DB", "itsm_dw"),
    user=os.getenv("POSTGRES_USER", "itsm"),
    password=os.getenv("POSTGRES_PASSWORD", "itsm_dw_secret_2026"),
)
MODEL_PATH = os.getenv("ML_MODEL_PATH", "/app/models/model.joblib")


def _load_dataset():
    conn = psycopg2.connect(**PG_CONN)
    df = pd.read_sql("""
        SELECT dp.label AS priority_label, ft.urgency, ft.impact, ft.mttr_hours,
               dc.itil_type AS category_type,
               EXTRACT(HOUR  FROM ft.date_creation) AS hour_of_day,
               EXTRACT(DOW   FROM ft.date_creation) AS day_of_week,
               EXTRACT(MONTH FROM ft.date_creation) AS month
        FROM fact_tickets ft
        LEFT JOIN dim_priority dp ON ft.priority_id = dp.priority_id
        LEFT JOIN dim_category dc ON ft.category_id = dc.category_id
        WHERE ft.priority_id IS NOT NULL
        ORDER BY ft.date_creation;
    """, conn)
    conn.close()
    return df


def _generate_synthetic(n=6000):
    np.random.seed(42)
    categories = ["network", "hardware", "software", "security", "access"]
    _dist = {
        "Very High": dict(u=[5,5,4,4,3], uw=[.40,.30,.20,.07,.03],
                          i=[5,5,4,4,3], iw=[.40,.30,.20,.07,.03], mu=3,  sd=1.5),
        "High":      dict(u=[4,4,3,3,5], uw=[.30,.25,.25,.15,.05],
                          i=[4,4,3,3,5], iw=[.30,.25,.25,.15,.05], mu=7,  sd=3),
        "Medium":    dict(u=[3,3,2,4,2], uw=[.30,.25,.20,.15,.10],
                          i=[3,3,2,4,2], iw=[.30,.25,.20,.15,.10], mu=20, sd=8),
        "Low":       dict(u=[2,2,1,3,1], uw=[.30,.25,.25,.15,.05],
                          i=[2,2,1,3,1], iw=[.30,.25,.25,.15,.05], mu=50, sd=20),
        "Very Low":  dict(u=[1,1,2,1,2], uw=[.40,.30,.20,.08,.02],
                          i=[1,1,2,1,2], iw=[.40,.30,.20,.08,.02], mu=100,sd=40),
    }
    sla = {"Very High":4,"High":8,"Medium":24,"Low":72,"Very Low":168}
    rows = []
    for _ in range(n):
        p = np.random.choice(list(_dist.keys()), p=[.05,.15,.40,.30,.10])
        d = _dist[p]
        urg = int(np.random.choice(d["u"], p=d["uw"]))
        imp = int(np.random.choice(d["i"], p=d["iw"]))
        mttr = max(0.3, np.random.normal(d["mu"], d["sd"]))
        rows.append(dict(
            priority_label=p, urgency=urg, impact=imp,
            hour_of_day=np.random.randint(0,24),
            day_of_week=np.random.randint(0,7),
            month=np.random.randint(1,13),
            category_type=np.random.choice(categories),
            mttr_hours=round(mttr,2),
            sla_respected=int(mttr<=sla[p]),
        ))
    return pd.DataFrame(rows)


def _engineer(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["urgency_x_impact"] = df["urgency"] * df["impact"]
    df["urgency_sq"]       = df["urgency"] ** 2
    df["impact_sq"]        = df["impact"]  ** 2
    df["severity_score"]   = df["urgency"] + df["impact"]
    df["is_business_hours"]= ((df["hour_of_day"] >= 8) & (df["hour_of_day"] <= 18)).astype(int)
    df["is_weekend"]       = (df["day_of_week"] >= 5).astype(int)
    df["quarter"]          = ((df["month"] - 1) // 3 + 1).astype(int)
    return df


def _prepare(df):
    df = _engineer(df)
    df = pd.get_dummies(df, columns=["category_type"], drop_first=False)
    base = ["urgency","impact","urgency_x_impact","urgency_sq","impact_sq",
            "severity_score","hour_of_day","day_of_week","month",
            "is_business_hours","is_weekend","quarter"]
    feature_cols = base + [c for c in df.columns if c.startswith("category_type_")]
    X  = df[feature_cols].fillna(0)
    le = LabelEncoder()
    y  = le.fit_transform(df["priority_label"])
    ym = df["mttr_hours"].fillna(df["mttr_hours"].median())
    return X, y, ym, le, list(feature_cols)


def _train(X_tr, X_te, y_tr, y_te, X_full, y_full, le):
    candidates = {
        "LogisticRegression": Pipeline([
            ("scaler", StandardScaler()),
            ("clf",    LogisticRegression(max_iter=2000, class_weight="balanced",
                                          C=1.0, random_state=42)),
        ]),
        "RandomForest": RandomForestClassifier(
            n_estimators=300, max_depth=18, min_samples_split=4,
            min_samples_leaf=2, class_weight="balanced", random_state=42, n_jobs=-1,
        ),
        "GradientBoosting": GradientBoostingClassifier(
            n_estimators=200, max_depth=5, learning_rate=0.08,
            subsample=0.85, random_state=42,
        ),
        "XGBoost": XGBClassifier(
            n_estimators=300, max_depth=6, learning_rate=0.08,
            subsample=0.85, colsample_bytree=0.8, reg_alpha=0.1,
            use_label_encoder=False, eval_metric="mlogloss", random_state=42,
        ),
    }
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    results = {}
    for name, clf in candidates.items():
        cv_scores = cross_val_score(clf, X_full, y_full, cv=cv, scoring="f1_weighted", n_jobs=-1)
        clf.fit(X_tr, y_tr)
        y_pred = clf.predict(X_te)
        results[name] = {
            "model":            clf,
            "f1":               f1_score(y_te, y_pred, average="weighted"),
            "accuracy":         accuracy_score(y_te, y_pred),
            "balanced_accuracy":balanced_accuracy_score(y_te, y_pred),
            "cv_f1_mean":       cv_scores.mean(),
            "cv_f1_std":        cv_scores.std(),
        }
        logger.info("[%s] F1=%.4f CV=%.4f±%.4f", name, results[name]["f1"],
                    cv_scores.mean(), cv_scores.std())
    best = max(results, key=lambda k: results[k]["cv_f1_mean"])
    logger.info("Best: %s CV-F1=%.4f", best, results[best]["cv_f1_mean"])
    return best, results


def run_retraining():
    """Entry point — called as FastAPI BackgroundTask."""
    logger.info("Retraining v2 started")
    try:
        try:
            df = _load_dataset()
            if len(df) < 100:
                df = _generate_synthetic()
        except Exception as e:
            logger.warning("DW load failed (%s) — synthetic", e)
            df = _generate_synthetic()

        X, y, ym, le, feature_cols = _prepare(df)
        X_tr, X_te, y_tr, y_te, ym_tr, ym_te = train_test_split(
            X, y, ym, test_size=0.2, random_state=42, stratify=y
        )
        best_name, results = _train(X_tr, X_te, y_tr, y_te, X, y, le)
        best = results[best_name]

        # MTTR regressor
        mttr_reg = GradientBoostingRegressor(
            n_estimators=200, max_depth=4, learning_rate=0.08, subsample=0.85, random_state=42,
        )
        mttr_reg.fit(X_tr, ym_tr)
        mae = mean_absolute_error(ym_te, mttr_reg.predict(X_te))

        os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
        bundle = {
            "model":         best["model"],
            "mttr_model":    mttr_reg,
            "label_encoder": le,
            "feature_cols":  feature_cols,
            "metrics": {
                "algorithm":         best_name,
                "f1":                best["f1"],
                "accuracy":          best["accuracy"],
                "balanced_accuracy": best["balanced_accuracy"],
                "cv_f1_mean":        best["cv_f1_mean"],
                "cv_f1_std":         best["cv_f1_std"],
                "mae_mttr":          mae,
            },
            "trained_at": datetime.utcnow().isoformat(),
        }
        joblib.dump(bundle, MODEL_PATH)

        try:
            conn = psycopg2.connect(**PG_CONN)
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO ml_model_registry (algorithm, f1_score, accuracy, mae_mttr, trained_at)
                    VALUES (%s, %s, %s, %s, NOW()) ON CONFLICT DO NOTHING
                """, (best_name, best["f1"], best["accuracy"], mae))
            conn.commit()
            conn.close()
        except Exception as db_err:
            logger.warning("Registry insert failed: %s", db_err)

        logger.info("Retraining done — %s  F1=%.4f  CV=%.4f  MAE=%.2fh",
                    best_name, best["f1"], best["cv_f1_mean"], mae)
    except Exception as e:
        logger.error("Retraining failed: %s", e, exc_info=True)
