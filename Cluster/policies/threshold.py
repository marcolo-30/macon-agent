# File: threshold.py
# Política REACTIVA: migra en cuanto detecta degradación, vuelve en cuanto r3 se recupera.
# Filosofía: OR logic → reacciona a cualquier señal de problema.
# Resultado esperado: 20+ migraciones por hora, violations < 150s.
#
# Métricas usadas (definidas en metrics.conf):
#   qos               → composite QoS score [0.0-1.0]
#   qos_latency_score → latency sub-score   [0.0-1.0]
#   nodes_cpu         → {node_name: cpu_%}

class CustomPolicy(Policy):
    # Umbrales locales — más agresivos que los defaults del agente
    MIGRATE_QOS_THRESHOLD     = 0.85   # (agent default: 0.80) — dispara antes
    MIGRATE_LATENCY_THRESHOLD = 0.35   # latencia ya sufriendo
    RETURN_CPU_THRESHOLD      = 55.0   # % CPU r3-node para volver (agent default: 50%)

    def should_migrate(self):
        """Decide SI hay que migrar. Lógica OR: cualquier señal dispara la acción."""
        knowledge    = self.agent.knowledge
        current_node = self.agent.current_node

        if current_node == "r3-node":
            qos           = knowledge.get("qos")           if knowledge.get("qos")           is not None else 1.0
            latency_score = knowledge.get("qos_latency_score") if knowledge.get("qos_latency_score") is not None else 1.0

            # OR: basta con que falle UNA condición para migrar
            migrate = qos < self.MIGRATE_QOS_THRESHOLD or latency_score < self.MIGRATE_LATENCY_THRESHOLD
            if migrate:
                print(f"[Analyze] threshold.py: QoS={qos:.3f}, latency={latency_score:.3f} → MIGRAR (OR trigger)")
            return migrate

        elif current_node == "vm1node":
            nodes_cpu = knowledge.get("nodes_cpu") or {}
            r3_cpu    = nodes_cpu.get("r3-node", 100.0)
            # Volver en cuanto r3 muestre recuperación (umbral más permisivo)
            should_return = r3_cpu < self.RETURN_CPU_THRESHOLD
            if should_return:
                print(f"[Analyze] threshold.py: r3_cpu={r3_cpu:.1f}% < {self.RETURN_CPU_THRESHOLD}% → VOLVER")
            return should_return

        return False

    def select_target_node(self):
        """Selecciona el nodo destino."""
        if self.agent.current_node == "r3-node":
            return "vm1node"
        else:
            return "r3-node"

    def get_reason(self, target_node):
        if target_node == "vm1node":
            return "QoS or latency below threshold — reactive migration."
        else:
            return "r3-node CPU recovered — returning immediately (reactive)."
