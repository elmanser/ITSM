"""
ML Training Pipeline — Priority Classification + MTTR Regression
Improvements v2: feature engineering, better hyperparameters, correlated
synthetic data, StratifiedKFold cross-validation, Pipeline with scaling.
"""
import json, logging, os, sys
from datetime import datetime

import joblib
import numpy as np
import pandas as pd
import psycopg2
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (accuracy_score, balanced_accuracy_score,
                              classification_report, f1_score, mean_absolute_error)
from sklearn.model_selection import StratifiedKFold, cross_val_score, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import LabelEncoder, StandardScaler
from xgboost import XGBClassifier

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ml_trainer")

PG_CONN = dict(
    host=os.getenv("POSTGRES_HOST", "postgres"),
    port=int(os.getenv("POSTGRES_PORT", "5432")),
    dbname=os.getenv("POSTGRES_DB", "itsm_dw"),
    user=os.getenv("POSTGRES_USER", "itsm"),
    password=os.getenv("POSTGRES_PASSWORD", "itsm_dw_secret_2026"),
)
MODEL_PATH  = os.getenv("ML_MODEL_PATH", "/app/models/model.joblib")
REPORT_PATH = os.path.dirname(MODEL_PATH)


# ── Data loading ──────────────────────────────────────────────────────────────
def load_dataset():
    conn = psycopg2.connect(**PG_CONN)
    df = pd.read_sql("""
        SELECT dp.label AS priority_label,
               ft.urgency, ft.impact, ft.mttr_hours,
               COALESCE(dc.name, 'unknown') AS category_type,
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
    logger.info("Loaded %d records from DW", len(df))
    return df


# ── Synthetic data (correlated features → better signal) ──────────────────────
def generate_synthetic_dataset(n=6000):
    """Synthetic dataset with urgency/impact strongly correlated to priority."""
    logger.info("Generating %d synthetic tickets with correlated features.", n)
    np.random.seed(42)
    categories = ["network", "hardware", "software", "security", "access", "database", "email"]

    # urgency/impact distributions per priority (realistic signal)
    _dist = {
        "Very High": dict(p=[0.05, 0.15, 0.40, 0.30, 0.10],
                          u_choices=[5, 5, 4, 4, 3], u_w=[.40, .30, .20, .07, .03],
                          i_choices=[5, 5, 4, 4, 3], i_w=[.40, .30, .20, .07, .03],
                          mttr_mu=3,   mttr_sd=1.5),
        "High":      dict(p=None,
                          u_choices=[4, 4, 3, 3, 5], u_w=[.30, .25, .25, .15, .05],
                          i_choices=[4, 4, 3, 3, 5], i_w=[.30, .25, .25, .15, .05],
                          mttr_mu=7,   mttr_sd=3),
        "Medium":    dict(p=None,
                          u_choices=[3, 3, 2, 4, 2], u_w=[.30, .25, .20, .15, .10],
                          i_choices=[3, 3, 2, 4, 2], i_w=[.30, .25, .20, .15, .10],
                          mttr_mu=20,  mttr_sd=8),
        "Low":       dict(p=None,
                          u_choices=[2, 2, 1, 3, 1], u_w=[.30, .25, .25, .15, .05],
                          i_choices=[2, 2, 1, 3, 1], i_w=[.30, .25, .25, .15, .05],
                          mttr_mu=50,  mttr_sd=20),
        "Very Low":  dict(p=None,
                          u_choices=[1, 1, 2, 1, 2], u_w=[.40, .30, .20, .08, .02],
                          i_choices=[1, 1, 2, 1, 2], i_w=[.40, .30, .20, .08, .02],
                          mttr_mu=100, mttr_sd=40),
    }
    priorities = list(_dist.keys())
    probs = [0.05, 0.15, 0.40, 0.30, 0.10]

    sla_limits = {"Very High": 4, "High": 8, "Medium": 24, "Low": 72, "Very Low": 168}
    rows = []
    for _ in range(n):
        prio = np.random.choice(priorities, p=probs)
        d    = _dist[prio]
        urg  = np.random.choice(d["u_choices"], p=d["u_w"])
        imp  = np.random.choice(d["i_choices"], p=d["i_w"])
        mttr = max(0.3, np.random.normal(d["mttr_mu"], d["mttr_sd"]))
        rows.append(dict(
            priority_label=prio,
            urgency=int(urg), impact=int(imp),
            hour_of_day=np.random.randint(0, 24),
            day_of_week=np.random.randint(0, 7),
            month=np.random.randint(1, 13),
            category_type=np.random.choice(categories),
            mttr_hours=round(mttr, 2),
            sla_respected=int(mttr <= sla_limits[prio]),
        ))
    return pd.DataFrame(rows)


# ── Feature engineering ────────────────────────────────────────────────────────
def _add_engineered_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add interaction and temporal features to improve model signal."""
    df = df.copy()
    # Interaction term: most predictive for priority
    df["urgency_x_impact"]  = df["urgency"] * df["impact"]
    # Non-linear terms
    df["urgency_sq"]         = df["urgency"] ** 2
    df["impact_sq"]          = df["impact"]  ** 2
    # Combined severity score
    df["severity_score"]     = df["urgency"] + df["impact"]
    # Temporal features
    df["is_business_hours"]  = ((df["hour_of_day"] >= 8) & (df["hour_of_day"] <= 18)).astype(int)
    df["is_weekend"]         = (df["day_of_week"] >= 5).astype(int)
    df["quarter"]            = ((df["month"] - 1) // 3 + 1).astype(int)
    return df


def prepare_features(df: pd.DataFrame):
    """Build feature matrix X and targets y_priority, y_mttr."""
    df = _add_engineered_features(df)
    df = pd.get_dummies(df, columns=["category_type"], drop_first=False)

    base_cols = [
        "urgency", "impact",
        "urgency_x_impact", "urgency_sq", "impact_sq", "severity_score",
        "hour_of_day", "day_of_week", "month",
        "is_business_hours", "is_weekend", "quarter",
    ]
    feature_cols = base_cols + [c for c in df.columns if c.startswith("category_type_")]
    X = df[feature_cols].fillna(0)

    le = LabelEncoder()
    y_priority = le.fit_transform(df["priority_label"])
    y_mttr     = df["mttr_hours"].fillna(df["mttr_hours"].median())

    return X, y_priority, y_mttr, le, list(feature_cols)


# ── Model training with cross-validation ─────────────────────────────────────
def train_and_evaluate(X_train, X_test, y_train, y_test, X_full, y_full, le):
    """Train 4 classifiers, evaluate with StratifiedKFold + hold-out. Return best by F1."""
    candidates = {
        "LogisticRegression": Pipeline([
            ("scaler", StandardScaler()),
            ("clf", LogisticRegression(
                max_iter=2000, class_weight="balanced",
                C=1.0, solver="lbfgs", multi_class="auto", random_state=42,
            )),
        ]),
        "RandomForest": RandomForestClassifier(
            n_estimators=300, max_depth=18, min_samples_split=4,
            min_samples_leaf=2, class_weight="balanced",
            random_state=42, n_jobs=-1,
        ),
        "GradientBoosting": GradientBoostingClassifier(
            n_estimators=200, max_depth=5, learning_rate=0.08,
            subsample=0.85, random_state=42,
        ),
        "XGBoost": XGBClassifier(
            n_estimators=300, max_depth=6, learning_rate=0.08,
            subsample=0.85, colsample_bytree=0.8, reg_alpha=0.1,
            use_label_encoder=False, eval_metric="mlogloss",
            random_state=42,
        ),
    }

    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    results = {}

    for name, clf in candidates.items():
        # StratifiedKFold CV score
        cv_scores = cross_val_score(clf, X_full, y_full, cv=cv,
                                    scoring="f1_weighted", n_jobs=-1)
        # Train on train split, evaluate on test split
        clf.fit(X_train, y_train)
        y_pred   = clf.predict(X_test)
        f1       = f1_score(y_test, y_pred, average="weighted")
        acc      = accuracy_score(y_test, y_pred)
        bal_acc  = balanced_accuracy_score(y_test, y_pred)

        results[name] = {
            "model":    clf,
            "f1":       f1,
            "accuracy": acc,
            "balanced_accuracy": bal_acc,
            "cv_f1_mean": cv_scores.mean(),
            "cv_f1_std":  cv_scores.std(),
            "report": classification_report(y_test, y_pred, target_names=le.classes_),
        }
        logger.info(
            "[%s] F1=%.4f  Acc=%.4f  BalAcc=%.4f  CV-F1=%.4f±%.4f",
            name, f1, acc, bal_acc, cv_scores.mean(), cv_scores.std(),
        )

    # Choose best by CV F1 (more reliable than single split)
    best_name = max(results, key=lambda k: results[k]["cv_f1_mean"])
    logger.info("Best model: %s (CV-F1=%.4f)", best_name, results[best_name]["cv_f1_mean"])
    return best_name, results


# ── Persistence ───────────────────────────────────────────────────────────────
def save_model(model, label_encoder, feature_cols, metrics, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    bundle = {
        "model":         model,
        "label_encoder": label_encoder,
        "feature_cols":  feature_cols,
        "metrics":       metrics,
        "trained_at":    datetime.utcnow().isoformat(),
    }
    joblib.dump(bundle, path)
    logger.info("Model saved to %s", path)


def save_report(results, report_dir):
    os.makedirs(report_dir, exist_ok=True)
    report = {
        name: {
            "f1": r["f1"], "accuracy": r["accuracy"],
            "balanced_accuracy": r["balanced_accuracy"],
            "cv_f1_mean": r["cv_f1_mean"], "cv_f1_std": r["cv_f1_std"],
            "report": r["report"],
        }
        for name, r in results.items()
    }
    path = os.path.join(report_dir, "evaluation_report.json")
    with open(path, "w") as f:
        json.dump(report, f, indent=2)
    logger.info("Evaluation report → %s", path)


# ── MTTR regression ───────────────────────────────────────────────────────────
def train_mttr_regressor(X_train, X_test, y_mttr_train, y_mttr_test):
    """Simple MTTR regression — returns MAE and fitted model."""
    from sklearn.ensemble import GradientBoostingRegressor
    from sklearn.pipeline import Pipeline as RPipeline
    reg = GradientBoostingRegressor(
        n_estimators=200, max_depth=4, learning_rate=0.08,
        subsample=0.85, random_state=42,
    )
    reg.fit(X_train, y_mttr_train)
    y_pred = reg.predict(X_test)
    mae = mean_absolute_error(y_mttr_test, y_pred)
    logger.info("MTTR Regressor MAE=%.2f h", mae)
    return reg, mae


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    logger.info("=" * 60)
    logger.info("ITSM ML Trainer v2 — Priority Classification + MTTR")
    logger.info("=" * 60)

    try:
        df_real = load_dataset()
        df_synth = generate_synthetic_dataset(n=15000)
        if len(df_real) >= 500:
            # Only blend real data once we have enough correlated data (post-producer-fix).
            df = pd.concat([df_real, df_synth], ignore_index=True)
            logger.info("Blended %d real + %d synthetic = %d total samples.",
                        len(df_real), len(df_synth), len(df))
        else:
            df = df_synth
            logger.info("Using pure synthetic data (DW has %d rows, threshold=500).", len(df_real))
    except Exception as e:
        logger.warning("DW load failed (%s) — using synthetic data.", e)
        df = generate_synthetic_dataset(n=12000)

    X, y_priority, y_mttr, le, feature_cols = prepare_features(df)
    logger.info("Features: %d | Samples: %d | Classes: %s",
                len(feature_cols), len(X), list(le.classes_))

    X_train, X_test, yp_train, yp_test, ym_train, ym_test = train_test_split(
        X, y_priority, y_mttr, test_size=0.2, random_state=42, stratify=y_priority
    )

    best_name, results = train_and_evaluate(
        X_train, X_test, yp_train, yp_test, X, y_priority, le
    )
    save_report(results, REPORT_PATH)

    # MTTR regressor
    mttr_model, mae = train_mttr_regressor(X_train, X_test, ym_train, ym_test)

    best = results[best_name]
    metrics = {
        "algorithm":        best_name,
        "f1":               best["f1"],
        "accuracy":         best["accuracy"],
        "balanced_accuracy": best["balanced_accuracy"],
        "cv_f1_mean":       best["cv_f1_mean"],
        "cv_f1_std":        best["cv_f1_std"],
        "mae_mttr":         mae,
    }

    # Save bundle with both models
    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    bundle = {
        "model":         best["model"],
        "mttr_model":    mttr_model,
        "label_encoder": le,
        "feature_cols":  feature_cols,
        "metrics":       metrics,
        "trained_at":    datetime.utcnow().isoformat(),
    }
    joblib.dump(bundle, MODEL_PATH)
    logger.info("Bundle saved → %s", MODEL_PATH)
    logger.info("Training complete — F1=%.4f  Acc=%.4f  CV-F1=%.4f±%.4f  MAE=%.2fh",
                best["f1"], best["accuracy"], best["cv_f1_mean"], best["cv_f1_std"], mae)


if __name__ == "__main__":
    main()
