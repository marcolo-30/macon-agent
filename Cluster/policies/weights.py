# File: weights.py
# WEIGHTED PRESSURE POLICY  (fixed for QoS 0-100 scale)
#
# MIGRATE from r3:
#   Weighted degradation pressure (QoS + CPU + latency) > MIGRATE_TRIGGER
#
# RETURN to r3:
#   r3 pressure < RETURN_TRIGGER  (hysteresis gap prevents oscillation)
#
# Metrics used (defined in metrics.conf):
#   qos               -> composite QoS [0-100]   ← normalised to [0-1] internally
#   qos_latency_score -> latency sub-score [0-1]
#   nodes_cpu         -> {node_name: cpu_%}

# ── Migration weights (must sum to 1.0) ───────────────────────────────────────
W_QOS     = 0.50   # composite QoS (primary service-quality signal)
W_CPU     = 0.30   # node CPU usage
W_LATENCY = 0.20   # latency sub-score

CPU_NORMAL      = 40.0  # % CPU considered "no load"
MIGRATE_TRIGGER = 0.35  # pressure score threshold to trigger migration  [0-1]
RETURN_TRIGGER  = 0.15  # pressure score to allow return (hysteresis gap) [0-1]


class CustomPolicy(Policy):

    # ── Weighted pressure score ───────────────────────────────────────────────
    def _pressure_score(self, qos_100, node_cpu, latency_score):
        """
        All inputs normalised to [0,1] before weighting.
          qos_100      : QoS composite  [0-100]  → normalised to [0,1]
          node_cpu     : node CPU usage [0-100 %]
          latency_score: latency score  [0-1]
        Returns total pressure [0-1] and individual stress components.
        """
        qos_norm       = qos_100 / 100.0
        qos_stress     = max(0.0, 1.0 - qos_norm)
        cpu_stress     = max(0.0, (node_cpu - CPU_NORMAL) / (100.0 - CPU_NORMAL))
        latency_stress = max(0.0, 1.0 - latency_score)

        score = W_QOS * qos_stress + W_CPU * cpu_stress + W_LATENCY * latency_stress
        return round(score, 4), qos_stress, cpu_stress, latency_stress

    # ── Main decision ─────────────────────────────────────────────────────────
    def should_migrate(self):
        current_node = self.agent.current_node
        knowledge    = self.agent.knowledge

        # ── On r3-node: migrate when pressure exceeds trigger ─────────────────
        if current_node == "r3-node":
            qos           = knowledge.get("qos")               if knowledge.get("qos")               is not None else 100.0
            latency_score = knowledge.get("qos_latency_score") if knowledge.get("qos_latency_score") is not None else 1.0
            nodes_cpu     = knowledge.get("nodes_cpu") or {}
            node_cpu      = nodes_cpu.get("r3-node", 0.0)

            score, qs, cs, ls = self._pressure_score(qos, node_cpu, latency_score)

            print(f"[Analyze] weights: pressure={score:.3f} "
                  f"[qos_stress={qs:.2f}x{W_QOS} + cpu_stress={cs:.2f}x{W_CPU} + "
                  f"lat_stress={ls:.2f}x{W_LATENCY}] trigger={MIGRATE_TRIGGER}")

            if score > MIGRATE_TRIGGER:
                print(f"[Analyze] weights: {score:.3f} > {MIGRATE_TRIGGER} → MIGRATE to vm1")
                return True
            return False

        # ── On vm1node: return when r3 pressure drops below return trigger ────
        elif current_node == "vm1node":
            nodes_cpu     = knowledge.get("nodes_cpu") or {}
            node_cpu_r3   = nodes_cpu.get("r3-node", 100.0)
            qos           = knowledge.get("qos")               if knowledge.get("qos")               is not None else 100.0
            latency_score = knowledge.get("qos_latency_score") if knowledge.get("qos_latency_score") is not None else 1.0

            r3_pressure, _, _, _ = self._pressure_score(qos, node_cpu_r3, latency_score)

            print(f"[Analyze] weights: r3_pressure={r3_pressure:.3f} "
                  f"(return_trigger={RETURN_TRIGGER})")

            if r3_pressure < RETURN_TRIGGER:
                print(f"[Analyze] weights: r3 recovered ({r3_pressure:.3f} < {RETURN_TRIGGER}) → RETURN to r3")
                return True
            return False

        return False

    # ── Target node selection ─────────────────────────────────────────────────
    def select_target_node(self):
        if self.agent.current_node == "r3-node":
            return "vm1node"
        else:
            return "r3-node"

    def get_reason(self, target_node):
        if target_node == "vm1node":
            return (f"Weighted pressure={MIGRATE_TRIGGER} exceeded "
                    f"(QoS x{W_QOS} + CPU x{W_CPU} + Latency x{W_LATENCY}).")
        else:
            return f"r3 pressure dropped below {RETURN_TRIGGER} — safe to return."
