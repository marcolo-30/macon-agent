#!/usr/bin/env python3
"""
train_model.py  v2 — QoS Regressor (Option A)
==============================================
Trains a RandomForest regressor to predict qos_composite on r3-node
from system and app metrics that are available regardless of pod location.

Inference logic (ml_based.py):
    predicted_qos_r3 = model.predict(features)
    on r3  → migrate to vm1  if predicted_qos_r3 < MIGRATE_THRESHOLD
    on vm1 → return  to r3   if predicted_qos_r3 > RETURN_THRESHOLD

Usage
─────
  python Tests/train_model.py --data Tests/training_data/overnight_YYYYMMDD_HHMM.csv

Outputs
───────
  Cluster/models/qos_regressor.pkl    trained model
  Cluster/models/features.json        feature list in training order
  Cluster/models/thresholds.json      migrate / return thresholds
  Cluster/models/training_report.txt  metrics + feature importances

Requirements: pip install scikit-learn pandas joblib
"""

import argparse
import json
import os

import joblib
import pandas as pd
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.preprocessing import StandardScaler
import math

# ── Paths ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
MODELS_DIR  = os.path.join(SCRIPT_DIR, "../Cluster/models")
os.makedirs(MODELS_DIR, exist_ok=True)

# ── Features & Target ─────────────────────────────────────────────────────────
# Primary features: always available regardless of pod node
FEATURES = [
    # ONLY system-level metrics — always available regardless of pod node.
    # App-level qos_* features are EXCLUDED because:
    #   1. They are components of the qos formula (model would cheat)
    #   2. When pod is on vm1 they reflect vm1, not r3
    "cpu_r3",        # key predictor — r3 node CPU, always available
    "cpu_vm1",       # context — other node load
    "cpu_r3_trend",  # is r3 CPU rising or falling?
    "cpu_vm1_trend", # is vm1 CPU rising or falling?
]
TARGET   = "qos"

# ── Decision thresholds (tunable, don't require retraining) ───────────────────
MIGRATE_THRESHOLD = 70.0   # migrate r3→vm1 when predicted_qos_r3 < this
RETURN_THRESHOLD  = 80.0   # return  vm1→r3 when predicted_qos_r3 > this
# Hysteresis gap (80-70=10 pts) prevents oscillation


def load_data(csv_path: str) -> pd.DataFrame:
    print(f"Loading data from {csv_path} ...")
    df = pd.read_csv(csv_path)
    print(f"  Raw rows     : {len(df)}")
    print(f"  Columns      : {list(df.columns)}")

    # Drop rows missing target or primary system features
    required = [TARGET, "cpu_r3"]
    df = df.dropna(subset=required)
    print(f"  After dropna : {len(df)} rows")

    # Fill trend NaNs with 0 (first 30 s of each session have no trend)
    trend_cols = [c for c in FEATURES if "trend" in c]
    df[trend_cols] = df[trend_cols].fillna(0.0)

    # Fill remaining app feature NaNs with column median
    df[FEATURES] = df[FEATURES].fillna(df[FEATURES].median())

    print(f"  QoS range    : {df[TARGET].min():.1f} – {df[TARGET].max():.1f}")
    print(f"  QoS mean     : {df[TARGET].mean():.1f}  std={df[TARGET].std():.1f}")
    return df


def train(df: pd.DataFrame) -> tuple:
    X = df[FEATURES]
    y = df[TARGET]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    print(f"\nTraining on {len(X_train)} samples, testing on {len(X_test)} ...")

    model = RandomForestRegressor(
        n_estimators  = 200,
        max_depth     = 12,
        min_samples_leaf = 4,
        n_jobs        = -1,
        random_state  = 42,
    )
    model.fit(X_train, y_train)

    # Evaluate
    y_pred  = model.predict(X_test)
    rmse    = math.sqrt(mean_squared_error(y_test, y_pred))
    mae     = mean_absolute_error(y_test, y_pred)
    r2      = r2_score(y_test, y_pred)

    print(f"\n── Test set metrics ────────────────────────────")
    print(f"  RMSE : {rmse:.2f}  (QoS points, 0-100 scale)")
    print(f"  MAE  : {mae:.2f}")
    print(f"  R²   : {r2:.4f}  (1.0 = perfect)")

    # Cross-validation
    cv_scores = cross_val_score(model, X, y, cv=5, scoring="r2")
    print(f"  CV R²: {cv_scores.mean():.4f} ± {cv_scores.std():.4f} (5-fold)")

    # Feature importances
    importances = sorted(
        zip(FEATURES, model.feature_importances_),
        key=lambda x: x[1], reverse=True
    )
    print(f"\n── Feature importances ─────────────────────────")
    for feat, imp in importances:
        bar = "█" * int(imp * 40)
        print(f"  {feat:<28} {imp:.4f}  {bar}")

    # Threshold simulation
    print(f"\n── Threshold simulation ────────────────────────")
    print(f"  MIGRATE_THRESHOLD : {MIGRATE_THRESHOLD}  "
          f"(migrate r3→vm1 when predicted_qos < this)")
    print(f"  RETURN_THRESHOLD  : {RETURN_THRESHOLD}  "
          f"(return  vm1→r3 when predicted_qos > this)")

    would_migrate = (y_pred < MIGRATE_THRESHOLD).sum()
    would_stay    = (y_pred >= MIGRATE_THRESHOLD).sum()
    print(f"  On test set: {would_migrate} samples would trigger migration, "
          f"{would_stay} would stay")

    return model, rmse, mae, r2, cv_scores, importances, y_pred, y_test


def save_artifacts(model, rmse, mae, r2, cv_scores, importances):
    # Model
    model_path = os.path.join(MODELS_DIR, "qos_regressor.pkl")
    joblib.dump(model, model_path)
    print(f"\n✅  Model saved  → {model_path}")

    # Feature list (order matters for inference)
    features_path = os.path.join(MODELS_DIR, "features.json")
    with open(features_path, "w") as f:
        json.dump(FEATURES, f, indent=2)
    print(f"✅  Features     → {features_path}")

    # Thresholds
    thresholds_path = os.path.join(MODELS_DIR, "thresholds.json")
    with open(thresholds_path, "w") as f:
        json.dump({
            "migrate_threshold": MIGRATE_THRESHOLD,
            "return_threshold":  RETURN_THRESHOLD,
            "description": (
                "migrate r3→vm1 when predicted_qos_r3 < migrate_threshold; "
                "return vm1→r3 when predicted_qos_r3 > return_threshold"
            )
        }, f, indent=2)
    print(f"✅  Thresholds   → {thresholds_path}")

    # Training report
    report_path = os.path.join(MODELS_DIR, "training_report.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("QoS Regressor — Training Report\n")
        f.write("=" * 50 + "\n\n")
        f.write(f"Target   : {TARGET} (0-100 scale)\n")
        f.write(f"Features : {len(FEATURES)}\n\n")
        f.write(f"RMSE     : {rmse:.2f}\n")
        f.write(f"MAE      : {mae:.2f}\n")
        f.write(f"R²       : {r2:.4f}\n")
        f.write(f"CV R²    : {cv_scores.mean():.4f} ± {cv_scores.std():.4f}\n\n")
        f.write("Feature Importances:\n")
        for feat, imp in importances:
            f.write(f"  {feat:<28} {imp:.4f}\n")
        f.write(f"\nThresholds:\n")
        f.write(f"  migrate r3→vm1 : predicted_qos < {MIGRATE_THRESHOLD}\n")
        f.write(f"  return  vm1→r3 : predicted_qos > {RETURN_THRESHOLD}\n")
    print(f"✅  Report       → {report_path}")


def main():
    global MIGRATE_THRESHOLD, RETURN_THRESHOLD

    ap = argparse.ArgumentParser(description="Train QoS regressor v2")
    ap.add_argument("--data", required=True,
                    help="Path to overnight CSV from data_collector.py")
    ap.add_argument("--migrate-threshold", type=float, default=MIGRATE_THRESHOLD)
    ap.add_argument("--return-threshold",  type=float, default=RETURN_THRESHOLD)
    args = ap.parse_args()

    MIGRATE_THRESHOLD = args.migrate_threshold
    RETURN_THRESHOLD  = args.return_threshold

    print("=" * 55)
    print("QoS REGRESSOR TRAINING  v2")
    print("=" * 55)

    df = load_data(args.data)

    if len(df) < 200:
        print(f"\n⚠️  Only {len(df)} samples — collect more data for a reliable model.")
        print("   Minimum recommended: 500 samples (≈2 sessions)")

    model, rmse, mae, r2, cv_scores, importances, y_pred, y_test = train(df)

    save_artifacts(model, rmse, mae, r2, cv_scores, importances)

    print(f"\n{'=' * 55}")
    print("DONE — next step:")
    print("  1. Check thresholds.json and tune if needed")
    print("  2. Set policy.conf → ml_based.py")
    print("  3. Restart cluster_agent.py")
    print(f"{'=' * 55}")


if __name__ == "__main__":
    main()
