# File: threshold.py
# Policy based on simple thresholds for QoS and CPU.
# Las métricas que usa (qos, qos_latency_score, qos_cpu_percent, nodes_cpu)
# se definen en metrics.conf — no aquí.

class CustomPolicy(Policy):
    def should_migrate(self):
        """Decides IF a migration is needed."""
        knowledge = self.agent.knowledge
        if self.agent.current_node == "r3-node":
            # Si qos es None, asumimos QoS buena (1.0) → no migrar
            qos = knowledge.get("qos") if knowledge.get("qos") is not None else 1.0
            latency_score = knowledge.get("qos_latency_score") if knowledge.get("qos_latency_score") is not None else 1.0
            # Migrar si el QoS compuesto cae bajo el umbral O si la latencia es muy mala
            migrate = qos < self.agent.qos_threshold or latency_score < 0.30
            if migrate:
                print(f"[Analyze] threshold.py: QoS={qos:.3f}, latency_score={latency_score:.3f} → migrar")
            return migrate
        elif self.agent.current_node == "vm1node":
            # Si nodes_cpu es None, asumimos r3-node ocupado (100%) → no volver
            nodes_cpu = knowledge.get("nodes_cpu") or {}
            return nodes_cpu.get("r3-node", 100.0) < self.agent.return_cpu_threshold
        return False

    def select_target_node(self):
        """Selects WHERE to migrate."""
        if self.agent.current_node == "r3-node":
            return "vm1node"
        else:
            return "r3-node"

    def get_reason(self, target_node):
        if target_node == "vm1node":
            knowledge = self.agent.knowledge
            qos = knowledge.get("qos", "?")
            lat = knowledge.get("qos_latency_score", "?")
            qos_str = f"{qos:.3f}" if isinstance(qos, float) else str(qos)
            lat_str = f"{lat:.3f}" if isinstance(lat, float) else str(lat)
            return f"QoS={qos_str} or latency_score={lat_str} below threshold ({self.agent.qos_threshold:.2f} / 0.30)"
        else:
            return f"Optimal node 'r3-node' is free (CPU < {self.agent.return_cpu_threshold}%)"
