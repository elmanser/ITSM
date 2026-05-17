"""Unit tests for ML training utilities."""
import pytest
import pandas as pd
import numpy as np

from train import generate_synthetic_dataset, prepare_features


class TestSyntheticDataset:
    def test_returns_dataframe(self):
        df = generate_synthetic_dataset(100)
        assert isinstance(df, pd.DataFrame)

    def test_correct_row_count(self):
        df = generate_synthetic_dataset(500)
        assert len(df) == 500

    def test_required_columns_present(self):
        df = generate_synthetic_dataset(100)
        required = ["priority_label", "urgency", "impact", "hour_of_day",
                    "day_of_week", "month", "category_type", "mttr_hours", "sla_respected"]
        for col in required:
            assert col in df.columns, f"Missing column: {col}"

    def test_priority_labels_are_valid(self):
        df = generate_synthetic_dataset(200)
        valid = {"Very High", "High", "Medium", "Low", "Very Low"}
        assert set(df["priority_label"].unique()).issubset(valid)

    def test_urgency_range(self):
        df = generate_synthetic_dataset(200)
        assert df["urgency"].between(1, 5).all()

    def test_impact_range(self):
        df = generate_synthetic_dataset(200)
        assert df["impact"].between(1, 5).all()

    def test_hour_of_day_range(self):
        df = generate_synthetic_dataset(200)
        assert df["hour_of_day"].between(0, 23).all()

    def test_month_range(self):
        df = generate_synthetic_dataset(200)
        assert df["month"].between(1, 12).all()

    def test_mttr_positive(self):
        df = generate_synthetic_dataset(200)
        assert (df["mttr_hours"] >= 0).all()

    def test_categories_are_valid(self):
        df = generate_synthetic_dataset(200)
        valid = {"network", "hardware", "software", "security", "access"}
        assert set(df["category_type"].unique()).issubset(valid)

    def test_reproducible_with_seed(self):
        df1 = generate_synthetic_dataset(100)
        df2 = generate_synthetic_dataset(100)
        # Both should be identical (numpy seed 42 hardcoded in function)
        pd.testing.assert_frame_equal(df1, df2)


class TestPrepareFeatures:
    def setup_method(self):
        self.df = generate_synthetic_dataset(300)

    def test_returns_four_objects(self):
        result = prepare_features(self.df.copy())
        assert len(result) == 5  # X, y_priority, y_mttr, le, feature_cols

    def test_X_has_no_nulls(self):
        X, _, _, _, _ = prepare_features(self.df.copy())
        assert not X.isnull().any().any()

    def test_feature_cols_include_base_features(self):
        _, _, _, _, feature_cols = prepare_features(self.df.copy())
        for col in ["urgency", "impact", "hour_of_day", "day_of_week", "month"]:
            assert col in feature_cols

    def test_feature_cols_include_one_hot_categories(self):
        _, _, _, _, feature_cols = prepare_features(self.df.copy())
        cat_features = [c for c in feature_cols if c.startswith("category_type_")]
        assert len(cat_features) > 0

    def test_y_priority_length_matches_X(self):
        X, y_priority, _, _, _ = prepare_features(self.df.copy())
        assert len(X) == len(y_priority)

    def test_label_encoder_covers_all_priorities(self):
        _, _, _, le, _ = prepare_features(self.df.copy())
        assert len(le.classes_) == 5

    def test_X_row_count_matches_input(self):
        X, _, _, _, _ = prepare_features(self.df.copy())
        assert len(X) == len(self.df)
