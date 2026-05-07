# File: weights.py
# Policy based on a weighted cost/benefit analysis.
# Las métricas que usa (qos, qos_latency_score, qos_cpu_percent, nodes_cpu)
# se definen en metrics.conf — no aquí.

class CustomPolicy(Policy):
    def should_migrate(self):
        """Decides IF a migration is needed."""
        current_node = self.agent.current_node
        knowledge = self.agent.knowledge

        if current_node == "r3-node":
            # Si qos es None → asumir QoS buena (1.0), no migrar
            qos = knowledge.get("qos") if knowledge.get("qos") is not None else 1.0
            # Usar CPU% directo de la app (más preciso que el nodo-level)
            app_cpu = knowledge.get("qos_cpu_percent") if knowledge.get("qos_cpu_percent") is not None else 0.0
            nodes_cpu = knowledge.get("nodes_cpu") or {}
            node_cpu = nodes_cpu.get("r3-node", 0.0)

            is_degraded = qos < 0.90
            # Sobrecarga: CPU de la app > 80% O CPU del nodo > 80%
            is_overloaded = app_cpu > 80.0 or node_cpu > 80.0
            if is_degraded and is_overloaded:
                print(f"[Analyze] weights.py: QoS={qos:.3f}, app_cpu={app_cpu:.1f}%, node_cpu={node_cpu:.1f}% → migrar")
            return is_degraded and is_overloaded

        elif current_node == "vm1node":
            nodes_cpu = knowledge.get("nodes_cpu") or {}
            is_safe_to_return = nodes_cpu.get("r3-node", 100.0) < self.agent.return_cpu_threshold
            if is_safe_to_return:
                # Toda la aritmética en escala [0,1]:
                #   current_score  = QoS actual  - coste del nodo actual  (vm1node → 0.50)
                #   destination_score = QoS máxima esperada - coste del destino (r3-node → 0.0)
                #   Usar qos_latency_score como proxy de QoS esperada en r3-node
                current_qos = knowledge.get("qos") if knowledge.get("qos") is not None else 0.0
                latency_score = knowledge.get("qos_latency_score") if knowledge.get("qos_latency_score") is not None else current_qos
                cost_of_current_node = self.agent.node_costs.get(current_node, 1.0)
                current_score = current_qos - cost_of_current_node

                # Si la latencia mejora en r3-node, el destination_score refleja eso
                expected_qos_r3 = min(1.0, latency_score + 0.10)  # r3 suele ser más rápido
                destination_score = expected_qos_r3 - self.agent.node_costs.get("r3-node", 0.0)

                benefit = destination_score - current_score - self.agent.migration_cost
                print(f"[Analyze] weights.py: Considering return. Benefit={benefit:.2f} "
                      f"(dest={destination_score:.2f}, curr={current_score:.2f}, cost={self.agent.migration_cost:.2f})")
                return benefit > 0

        return False

    def select_target_node(self):
        """Selects WHERE to migrate."""
        if self.agent.current_node == "r3-node":
            return "vm1node"
        else:
            return "r3-node"

    def get_reason(self, target_node):
        if target_node == "vm1node":
            return "QoS degradation and high CPU detected on optimal node."
        else:
            return "Cost/benefit analysis (with latency signal) favors returning to optimal node."
