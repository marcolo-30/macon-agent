# ml_based.py — QoS Regressor Migration Policy (Option A)
# =========================================================
# Uses a RandomForest regressor to predict what QoS the
# traffic-processor would get if it were running on r3-node,
# based on system + app metrics.
#
# Decision logic:
#   on r3-node : migrate to vm1  if predicted_qos_r3 < MIGRATE_THRESHOLD
#                                OR (cpu_r3 rising fast AND predicted_qos_r3 < EARLY_THRESHOLD)
#   on vm1node : return  to r3   if predicted_qos_r3 > RETURN_THRESHOLD
#
# Two independent cooldowns prevent different failure modes:
#   MIGRATE_COOLDOWN_S (60s)  — lets k8s reschedule and Prometheus catch up
#   RETURN_COOLDOWN_S  (180s) — prevents feedback loop: pod leaves r3 →
#                               cpu_r3 drops → model predicts high QoS →
#                               immediate return → cpu_r3 rises again
#
# Requires (in Cluster/models/):
#   qos_regressor.pkl   trained model
#   features.json       feature list in training order
#   thresholds.json     migrate / return thresholds

import collections
import json
import os
import sys
import time

# __file__ is unavailable when loaded via exec() — derive path from the
# running agent script (cluster_agent.py lives in Cluster/, models/ is a sibling)
SCRIPT_DIR   = os.path.dirname(os.path.abspath(sys.argv[0]))
MODELS_DIR   = os.path.join(SCRIPT_DIR, "models")
TREND_WINDOW = 6    # samples to compute rolling trend (matches data_collector)

# ── Cooldowns (independent per direction) ────────────────────────────────────
MIGRATE_COOLDOWN_S = 60   # after r3→vm1: wait for k8s + Prometheus to settle
RETURN_COOLDOWN_S  = 180  # after vm1→r3: wait to avoid pod-CPU feedback loop

# ── Early-warning trigger ─────────────────────────────────────────────────────
# If cpu_r3 is rising faster than this per sample (≈%/5s), migrate preemptively
# even if predicted QoS hasn't crossed the main threshold yet.
CPU_TREND_ALARM    = 2.0   # % per sample — rapid CPU ramp triggers early migration
EARLY_THRESHOLD    = 82.0  # predicted QoS must still be below this (not perfect)


class CustomPolicy(Policy):

    # ── Load model artifacts once at init ─────────────────────────────────────
    def __init__(self, agent):
        super().__init__(agent)
        import joblib, pandas as pd
        self._pd = pd

        model_path      = os.path.join(MODELS_DIR, "qos_regressor.pkl")
        features_path   = os.path.join(MODELS_DIR, "features.json")
        thresholds_path = os.path.join(MODELS_DIR, "thresholds.json")

        self._model    = joblib.load(model_path)
        with open(features_path)   as f: self._features   = json.load(f)
        with open(thresholds_path) as f: thresholds       = json.load(f)

        self.MIGRATE_THRESHOLD = thresholds["migrate_threshold"]
        self.RETURN_THRESHOLD  = thresholds["return_threshold"]

        # Rolling buffers for trend computation (same window as training)
        self._buf = {
            "qos":    collections.deque(maxlen=TREND_WINDOW + 1),
            "lat":    collections.deque(maxlen=TREND_WINDOW + 1),
            "cpu_r3": collections.deque(maxlen=TREND_WINDOW + 1),
            "cpu_vm1":collections.deque(maxlen=TREND_WINDOW + 1),
        }

        # Independent cooldown timestamps per direction
        self._last_migrate_time = 0.0  # when we last triggered r3 → vm1
        self._last_return_time  = 0.0  # when we last triggered vm1 → r3

        print(f"[ML Policy] model loaded | "
              f"migrate<{self.MIGRATE_THRESHOLD} | "
              f"return>{self.RETURN_THRESHOLD} | "
              f"migrate_cooldown={MIGRATE_COOLDOWN_S}s | "
              f"return_cooldown={RETURN_COOLDOWN_S}s")

    # ── Rolling trend helper ───────────────────────────────────────────────────
    def _update_trends(self):
        k   = self.agent.knowledge
        cpu = k.get("nodes_cpu") or {}

        self._buf["qos"].append(k.get("qos") or 100.0)
        self._buf["lat"].append(k.get("qos_latency_score") or 1.0)
        self._buf["cpu_r3"].append(cpu.get("r3-node") or 0.0)
        self._buf["cpu_vm1"].append(cpu.get("vm1node") or 0.0)

    def _trend(self, key: str) -> float:
        b = self._buf[key]
        if len(b) < TREND_WINDOW + 1:
            return 0.0
        return round((b[-1] - b[0]) / TREND_WINDOW, 4)

    # ── Build feature vector ───────────────────────────────────────────────────
    def _build_features(self) -> dict:
        cpu = self.agent.knowledge.get("nodes_cpu") or {}
        return {
            "cpu_r3":       cpu.get("r3-node") or 0.0,
            "cpu_vm1":      cpu.get("vm1node") or 0.0,
            "cpu_r3_trend": self._trend("cpu_r3"),
            "cpu_vm1_trend":self._trend("cpu_vm1"),
        }

    # ── Predict QoS on r3 ─────────────────────────────────────────────────────
    def _predict_qos_r3(self) -> float:
        self._update_trends()
        features = self._build_features()
        X = self._pd.DataFrame([features])[self._features]
        predicted = float(self._model.predict(X)[0])
        return round(max(0.0, min(100.0, predicted)), 2)

    # ── Migration decision ─────────────────────────────────────────────────────
    def should_migrate(self) -> bool:
        now     = time.time()
        current = self.agent.current_node

        # ── On r3-node: check migrate cooldown ────────────────────────────────
        if current == "r3-node":
            elapsed = now - self._last_migrate_time
            if elapsed < MIGRATE_COOLDOWN_S:
                remaining = int(MIGRATE_COOLDOWN_S - elapsed)
                print(f"[ML Policy] migrate cooldown {remaining}s remaining")
                return False

            predicted  = self._predict_qos_r3()
            cpu_trend  = self._trend("cpu_r3")

            # Standard threshold
            if predicted < self.MIGRATE_THRESHOLD:
                print(f"[ML Policy] on r3 | predicted={predicted:.1f} < "
                      f"{self.MIGRATE_THRESHOLD} → MIGRATE")
                self._last_migrate_time = now
                return True

            # Early-warning: CPU rising fast AND QoS already degrading
            if cpu_trend >= CPU_TREND_ALARM and predicted < EARLY_THRESHOLD:
                print(f"[ML Policy] on r3 | EARLY WARNING — "
                      f"cpu_trend={cpu_trend:.2f}%/sample >= {CPU_TREND_ALARM} AND "
                      f"predicted={predicted:.1f} < {EARLY_THRESHOLD} → preemptive MIGRATE")
                self._last_migrate_time = now
                return True

            print(f"[ML Policy] on r3 | predicted={predicted:.1f} | "
                  f"cpu_trend={cpu_trend:.2f} | stay")
            return False

        # ── On vm1node: check return cooldown ─────────────────────────────────
        elif current == "vm1node":
            elapsed = now - self._last_return_time
            if elapsed < RETURN_COOLDOWN_S:
                remaining = int(RETURN_COOLDOWN_S - elapsed)
                print(f"[ML Policy] return cooldown {remaining}s remaining")
                return False

            predicted = self._predict_qos_r3()

            if predicted > self.RETURN_THRESHOLD:
                print(f"[ML Policy] on vm1 | predicted={predicted:.1f} > "
                      f"{self.RETURN_THRESHOLD} → RETURN to r3")
                self._last_return_time = now
                return True

            print(f"[ML Policy] on vm1 | predicted={predicted:.1f} | "
                  f"r3 not ready, stay on vm1")
            return False

        return False

    # ── Target node ───────────────────────────────────────────────────────────
    def select_target_node(self) -> str:
        if self.agent.current_node == "r3-node":
            return "vm1node"
        return "r3-node"

    # ── Reason (for logs) ─────────────────────────────────────────────────────
    def get_reason(self, target_node: str) -> str:
        predicted  = self._predict_qos_r3()
        cpu_trend  = self._trend("cpu_r3")
        if target_node == "vm1node":
            if cpu_trend >= CPU_TREND_ALARM and predicted < EARLY_THRESHOLD:
                return (f"ML early-warning: cpu_trend={cpu_trend:.2f}%/sample "
                        f"AND predicted_qos={predicted:.1f} < {EARLY_THRESHOLD}")
            return (f"ML: predicted_qos_r3={predicted:.1f} < "
                    f"{self.MIGRATE_THRESHOLD} → offload to vm1")
        return (f"ML: predicted_qos_r3={predicted:.1f} > "
                f"{self.RETURN_THRESHOLD} → r3 recovered, returning")
