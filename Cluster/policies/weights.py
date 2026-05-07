# File: weights.py
# Política DELIBERADA: migra solo cuando la degradación es compound y sostenida.
# Filosofía: AND logic + cooldowns de permanencia mínima.
# Resultado esperado: 4-6 migraciones por hora, violations moderadas (300-600s).
#
# Mecanismo de cooldowns (estado en agent.knowledge):
#   _w_migrated_at  → timestamp de última salida de r3. Bloquea retorno por MIN_ON_VM1_S.
#   _w_returned_at  → timestamp de último retorno a r3. Bloquea re-migración por MIN_ON_R3_S.
#
# Métricas usadas (definidas en metrics.conf):
#   qos               → composite QoS score [0.0-1.0]
#   qos_latency_score → latency sub-score   [0.0-1.0]
#   nodes_cpu         → {node_name: cpu_%}

import time

# Permanencia mínima en cada nodo antes de permitir la siguiente migración.
# Con ciclos de stressor 300s-BURN / 300s-REST (600s total):
#   MIN_ON_VM1_S = 600  → se salta al menos un ciclo completo antes de volver
#   MIN_ON_R3_S  = 450  → tras volver, espera > 1 REST antes de migrar de nuevo
MIN_ON_VM1_S = 600   # segundos mínimos en vm1node tras migrar
MIN_ON_R3_S  = 450   # segundos mínimos en r3-node tras volver

# Umbrales del trigger (condición AND — ambos deben cumplirse)
DEGRADE_QOS_THRESHOLD = 0.85   # QoS composite debe caer bajo esto
OVERLOAD_CPU_NODE     = 72.0   # CPU% del nodo r3 debe superar esto

# Umbral de retorno
RETURN_R3_CPU_MAX     = 30.0   # r3-node debe estar por debajo para considerar vuelta


class CustomPolicy(Policy):

    def should_migrate(self):
        """Decide SI hay que migrar. Lógica AND + cooldowns de permanencia."""
        current_node = self.agent.current_node
        knowledge    = self.agent.knowledge
        now          = time.time()

        # ── Desde r3-node: evaluar si migrar a vm1node ─────────────────────────
        if current_node == "r3-node":
            # Cooldown: no migrar demasiado pronto tras un retorno reciente
            returned_at = knowledge.get("_w_returned_at", 0.0)
            elapsed_on_r3 = now - returned_at
            if elapsed_on_r3 < MIN_ON_R3_S:
                wait = MIN_ON_R3_S - elapsed_on_r3
                print(f"[Analyze] weights.py: En r3, cooldown retorno: {wait:.0f}s restantes → esperar")
                return False

            qos       = knowledge.get("qos")       if knowledge.get("qos")       is not None else 1.0
            nodes_cpu = knowledge.get("nodes_cpu") or {}
            node_cpu  = nodes_cpu.get("r3-node", 0.0)

            # AND: la degradación debe ser compound (QoS baja Y nodo sobrecargado)
            is_degraded   = qos      < DEGRADE_QOS_THRESHOLD
            is_overloaded = node_cpu > OVERLOAD_CPU_NODE

            if is_degraded and is_overloaded:
                print(f"[Analyze] weights.py: QoS={qos:.3f} < {DEGRADE_QOS_THRESHOLD}, "
                      f"node_cpu={node_cpu:.1f}% > {OVERLOAD_CPU_NODE}% → MIGRAR (AND trigger)")
                knowledge["_w_migrated_at"] = now   # registrar momento de salida
                return True

            print(f"[Analyze] weights.py: QoS={qos:.3f}, node_cpu={node_cpu:.1f}% "
                  f"(degraded={is_degraded}, overloaded={is_overloaded}) → mantener en r3")
            return False

        # ── Desde vm1node: evaluar si volver a r3-node ─────────────────────────
        elif current_node == "vm1node":
            # Cooldown: no volver antes de MIN_ON_VM1_S (asegura al menos un ciclo completo)
            migrated_at   = knowledge.get("_w_migrated_at", 0.0)
            elapsed_on_vm1 = now - migrated_at
            if elapsed_on_vm1 < MIN_ON_VM1_S:
                wait = MIN_ON_VM1_S - elapsed_on_vm1
                print(f"[Analyze] weights.py: En vm1, cooldown salida: {wait:.0f}s restantes → esperar")
                return False

            nodes_cpu = knowledge.get("nodes_cpu") or {}
            r3_cpu    = nodes_cpu.get("r3-node", 100.0)

            # Condición 1: r3 debe estar verdaderamente libre
            if r3_cpu >= RETURN_R3_CPU_MAX:
                print(f"[Analyze] weights.py: r3_cpu={r3_cpu:.1f}% >= {RETURN_R3_CPU_MAX}% → r3 aún ocupado")
                return False

            # Condición 2: análisis coste-beneficio (escala [0,1])
            current_qos   = knowledge.get("qos")               if knowledge.get("qos")               is not None else 0.0
            latency_score = knowledge.get("qos_latency_score") if knowledge.get("qos_latency_score") is not None else current_qos

            cost_current      = self.agent.node_costs.get(current_node, 1.0)   # vm1: 0.50
            current_score     = current_qos - cost_current

            expected_qos_r3   = min(1.0, latency_score + 0.10)                # r3 suele ser más rápido
            destination_score = expected_qos_r3 - self.agent.node_costs.get("r3-node", 0.0)

            benefit = destination_score - current_score - self.agent.migration_cost
            print(f"[Analyze] weights.py: r3_cpu={r3_cpu:.1f}%, benefit={benefit:.3f} "
                  f"(dest={destination_score:.2f}, curr={current_score:.2f}, "
                  f"migcost={self.agent.migration_cost:.2f}) → {'VOLVER' if benefit > 0 else 'quedarse en vm1'}")

            if benefit > 0:
                knowledge["_w_returned_at"] = now   # registrar momento de retorno
                return True

        return False

    def select_target_node(self):
        """Selecciona el nodo destino."""
        if self.agent.current_node == "r3-node":
            return "vm1node"
        else:
            return "r3-node"

    def get_reason(self, target_node):
        if target_node == "vm1node":
            return "Degradación compound: QoS bajo Y CPU de nodo sobrecargado (AND trigger)."
        else:
            return "Cooldown cumplido + análisis coste-beneficio favorable → retorno deliberado a nodo óptimo."
