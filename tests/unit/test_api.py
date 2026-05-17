"""Unit tests for FastAPI prediction service."""
import sys
import os
import numpy as np
from unittest.mock import MagicMock, patch

# ── Mock model bundle before importing main ───────────────────────────────────
_mock_model = MagicMock()
_mock_model.predict_proba.return_value = np.array([[0.05, 0.70, 0.10, 0.10, 0.05]])

_mock_le = MagicMock()
_mock_le.classes_ = ["High", "Low", "Medium", "Very High", "Very Low"]
_mock_le.inverse_transform.side_effect = lambda x: [_mock_le.classes_[i] for i in x]

# Must match the 17 engineered features in build_feature_vector
MOCK_BUNDLE = {
    "model": _mock_model,
    "label_encoder": _mock_le,
    "feature_cols": [
        "urgency", "impact",
        "urgency_x_impact", "urgency_sq", "impact_sq", "severity_score",
        "hour_of_day", "day_of_week", "month",
        "is_business_hours", "is_weekend", "quarter",
        "category_type_access", "category_type_hardware",
        "category_type_network", "category_type_security", "category_type_software",
    ],
    "metrics": {"algorithm": "RandomForest", "f1": 0.85, "accuracy": 0.82},
    "trained_at": "2026-01-01T00:00:00",
}

_p1 = patch("os.path.exists", return_value=True)
_p2 = patch("joblib.load", return_value=MOCK_BUNDLE)
_p1.start()
_p2.start()

from main import app, build_feature_vector, TicketPayload  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

_p1.stop()
_p2.stop()

client = TestClient(app)


class TestBuildFeatureVector:
    def _make_payload(self, **kwargs):
        defaults = dict(urgency=3, impact=3, hour_of_day=9,
                        day_of_week=1, month=5, category_type="network")
        defaults.update(kwargs)
        return TicketPayload(**defaults)

    def test_returns_dataframe_with_one_row(self):
        import pandas as pd
        payload = self._make_payload()
        df = build_feature_vector(payload)
        assert len(df) == 1

    def test_columns_match_feature_cols(self):
        payload = self._make_payload()
        df = build_feature_vector(payload)
        assert list(df.columns) == MOCK_BUNDLE["feature_cols"]

    def test_known_category_is_one_hot_encoded(self):
        payload = self._make_payload(category_type="network")
        df = build_feature_vector(payload)
        assert df["category_type_network"].iloc[0] == 1

    def test_unknown_category_stays_all_zeros(self):
        payload = self._make_payload(category_type="unknown_category")
        df = build_feature_vector(payload)
        cat_cols = [c for c in MOCK_BUNDLE["feature_cols"] if c.startswith("category_type_")]
        for col in cat_cols:
            assert df[col].iloc[0] == 0

    def test_numeric_features_copied_correctly(self):
        payload = self._make_payload(urgency=5, impact=4, hour_of_day=14, day_of_week=3, month=11)
        df = build_feature_vector(payload)
        assert df["urgency"].iloc[0] == 5
        assert df["impact"].iloc[0] == 4
        assert df["hour_of_day"].iloc[0] == 14
        assert df["day_of_week"].iloc[0] == 3
        assert df["month"].iloc[0] == 11


class TestHealthEndpoint:
    def test_returns_200(self):
        resp = client.get("/health")
        assert resp.status_code == 200

    def test_returns_status_ok(self):
        resp = client.get("/health")
        assert resp.json()["status"] == "ok"

    def test_returns_timestamp(self):
        resp = client.get("/health")
        assert "timestamp" in resp.json()


class TestPredictEndpoint:
    def _valid_payload(self):
        return {
            "urgency": 4,
            "impact": 5,
            "hour_of_day": 10,
            "day_of_week": 2,
            "month": 5,
            "category_type": "network",
        }

    def test_valid_request_returns_200(self):
        resp = client.post("/predict", json=self._valid_payload())
        assert resp.status_code == 200

    def test_response_has_predicted_label(self):
        resp = client.post("/predict", json=self._valid_payload())
        assert "predicted_label" in resp.json()

    def test_response_has_probabilities(self):
        resp = client.post("/predict", json=self._valid_payload())
        assert "probabilities" in resp.json()

    def test_urgency_out_of_range_returns_422(self):
        payload = self._valid_payload()
        payload["urgency"] = 10
        resp = client.post("/predict", json=payload)
        assert resp.status_code == 422

    def test_missing_field_returns_422(self):
        payload = self._valid_payload()
        del payload["category_type"]
        resp = client.post("/predict", json=payload)
        assert resp.status_code == 422

    def test_empty_category_returns_422(self):
        payload = self._valid_payload()
        payload["category_type"] = ""
        resp = client.post("/predict", json=payload)
        assert resp.status_code == 422


class TestMetadataEndpoint:
    def test_returns_200(self):
        resp = client.get("/metadata")
        assert resp.status_code == 200

    def test_returns_algorithm(self):
        resp = client.get("/metadata")
        assert "algorithm" in resp.json()

    def test_returns_features(self):
        resp = client.get("/metadata")
        assert "features" in resp.json()
        assert isinstance(resp.json()["features"], list)
