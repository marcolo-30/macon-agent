import spade
from spade.behaviour import PeriodicBehaviour, OneShotBehaviour
import datetime
import httpx
import json
import asyncio
import os
from kubernetes import client, config

# --- Configuration ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# --- Base Policy ---

class Policy:
    def __init__(self, agent):
        self.agent = agent
    def should_migrate(self):
        raise NotImplementedError
    def select_target_node(self):
        raise NotImplementedError
    def get_reason(self, target_node):
        return "Policy decision"

class PassivePolicy(Policy):
    def should_migrate(self):
        print("[Analyze] Passive policy active. No action will be taken.")
        return False
    def select_target_node(self):
        return None

# --- Agent Behaviours ---

class MonitorBehaviour(PeriodicBehaviour):

    # ── Kubernetes ─────────────────────────────────────────────────────────────
    def get_pod_node(self):
        try:
            v1 = client.CoreV1Api()
            pods = v1.list_namespaced_pod(
                namespace=self.agent.namespace,
                label_selector=self.agent.pod_label_selector,
            )
            if pods.items:
                return pods.items[0].spec.node_name
        except Exception:
            return None

    # ── Policy hot-reload ──────────────────────────────────────────────────────
    def check_for_policy_change(self):
        try:
            with open(self.agent.policy_conf_path, 'r') as f:
                policy_filename = f.read().strip()

            if not policy_filename:
                if not isinstance(self.agent.policy, PassivePolicy):
                    print("[Monitor] policy.conf vacío → PassivePolicy.")
                    self.agent.policy = PassivePolicy(self.agent)
                    self.agent.current_policy_filename = ""
                    self.agent.current_policy_mtime = 0.0
                return

            policy_path = os.path.join(SCRIPT_DIR, "policies", policy_filename)
            mtime = os.path.getmtime(policy_path)

            if policy_filename != self.agent.current_policy_filename or \
               mtime != self.agent.current_policy_mtime:
                reason = "nuevo archivo" if policy_filename != self.agent.current_policy_filename \
                         else "archivo editado"
                print(f"[Monitor] 🧠 Policy reload ({reason}): '{policy_filename}'...")
                with open(policy_path, 'r') as f:
                    code = f.read()
                scope = {'Policy': Policy}
                exec(code, scope)
                cls = scope.get('CustomPolicy')
                if cls:
                    self.agent.policy = cls(self.agent)
                    self.agent.current_policy_filename = policy_filename
                    self.agent.current_policy_mtime = mtime
                    print(f"[Monitor] ✅ Policy activa: '{policy_filename}'.")
                else:
                    print(f"[Monitor] ❌ '{policy_filename}' no define CustomPolicy.")
        except Exception as e:
            print(f"[Monitor] ❌ Error cargando policy: {e}")

    # ── Metrics hot-reload ─────────────────────────────────────────────────────
    def check_for_metrics_change(self):
        """
        Lee metrics.conf (JSON). Si cambió el archivo, actualiza agent.active_metrics.

        Formato metrics.conf:
        {
          "nombre_metrica": {
            "query": "promql...",
            "type": "scalar"          ← un solo float
          },
          "nodes_cpu": {
            "query": "promql...",
            "type": "per_node",       ← dict {node_name: float}
            "node_label": "k8s_node_name"
          }
        }
        """
        try:
            mtime = os.path.getmtime(self.agent.metrics_conf_path)
            if mtime == self.agent.current_metrics_mtime:
                return  # sin cambios

            with open(self.agent.metrics_conf_path, 'r') as f:
                new_metrics = json.load(f)

            self.agent.active_metrics = new_metrics
            self.agent.current_metrics_mtime = mtime
            names = list(new_metrics.keys())
            print(f"[Monitor] 📊 Metrics recargadas: {names}")
        except Exception as e:
            print(f"[Monitor] ❌ Error cargando metrics.conf: {e}")

    # ── Prometheus fetch ───────────────────────────────────────────────────────
    async def fetch_metrics(self):
        """
        Consulta todas las métricas definidas en agent.active_metrics y guarda
        los resultados en agent.knowledge.

        Tipos soportados:
          scalar   → knowledge[name] = float
          per_node → knowledge[name] = {node_name: float}
        """
        metrics = self.agent.active_metrics
        if not metrics:
            return

        async with httpx.AsyncClient() as http_client:
            coros = [
                http_client.get(
                    f"{self.agent.prometheus_url}/api/v1/query",
                    params={"query": cfg["query"]},
                )
                for cfg in metrics.values()
            ]
            names = list(metrics.keys())
            responses = await asyncio.gather(*coros, return_exceptions=True)

        for name, res, cfg in zip(names, responses, metrics.values()):
            if isinstance(res, Exception):
                print(f"[Monitor] ⚠️  Error fetching '{name}': {res}")
                self.agent.knowledge[name] = None
                continue

            prom_results = res.json().get('data', {}).get('result', [])
            if not prom_results:
                print(f"[Monitor] ⚠️  Sin datos para '{name}' (HTTP {res.status_code})")
                self.agent.knowledge[name] = None
                continue

            metric_type = cfg.get("type", "scalar")

            if metric_type == "per_node":
                label = cfg.get("node_label", "k8s_node_name")
                self.agent.knowledge[name] = {
                    r.get('metric', {}).get(label): float(r['value'][1])
                    for r in prom_results
                }
            else:  # scalar (default)
                self.agent.knowledge[name] = float(prom_results[0]['value'][1])

    # ── Main cycle ─────────────────────────────────────────────────────────────
    async def run(self):
        print("--- [Monitor] ---")
        self.check_for_policy_change()
        self.check_for_metrics_change()

        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(
                None, config.load_kube_config,
                self.agent.kubeconfig_path, self.agent.kube_context,
            )
            self.agent.current_node = (
                await loop.run_in_executor(None, self.get_pod_node)
                or self.agent.current_node
            )
        except Exception as e:
            print(f"[Monitor] ❌ Error obteniendo nodo del pod: {e}")
            return

        try:
            await self.fetch_metrics()
        except Exception as e:
            print(f"[Monitor] ❌ Error crítico en fetch_metrics: {e}")

        # Log resumen
        qos = self.agent.knowledge.get('qos')
        qos_str = f"{qos:.3f}" if isinstance(qos, float) else str(qos)
        cpu = self.agent.knowledge.get('nodes_cpu')
        cpu_str = ({k: f"{v:.1f}%" for k, v in cpu.items()} if cpu else "N/A")
        print(f"[Monitor] Knowledge: node={self.agent.current_node} | "
              f"QoS={qos_str} | NodesCPU={cpu_str}")
        # Log de otras métricas dinámicas (aparte de qos y nodes_cpu)
        extras = {k: v for k, v in self.agent.knowledge.items()
                  if k not in ('qos', 'nodes_cpu', 'target_node')}
        if extras:
            print(f"[Monitor] Extra metrics: {extras}")


class AnalyzeBehaviour(PeriodicBehaviour):
    async def run(self):
        print("--- [Analyze] ---")
        if self.agent.migration_in_flight or self.agent.current_node == "unknown":
            return
        if self.agent.policy.should_migrate():
            print("[Analyze] ‼️  Condición de migración cumplida. Iniciando Plan.")
            self.agent.migration_in_flight = True
            self.agent.add_behaviour(PlanBehaviour())


class PlanBehaviour(OneShotBehaviour):
    async def run(self):
        print("--- [Plan] ---")
        target_node = self.agent.policy.select_target_node()
        if target_node:
            reason = self.agent.policy.get_reason(target_node)
            print(f"[Plan] Nodo destino: '{target_node}'. Motivo: {reason}")
            self.agent.knowledge["target_node"] = target_node
            self.agent.add_behaviour(ExecuteBehaviour())
        else:
            print("[Plan] Sin nodo destino válido. Abortando migración.")
            self.agent.migration_in_flight = False


class ExecuteBehaviour(OneShotBehaviour):
    def patch_deployment(self, target_node):
        try:
            config.load_kube_config(
                config_file=self.agent.kubeconfig_path,
                context=self.agent.kube_context,
            )
            api = client.AppsV1Api()
            body = {"spec": {"template": {"spec": {
                "nodeSelector": {"kubernetes.io/hostname": target_node}
            }}}}
            api.patch_namespaced_deployment(
                name=self.agent.deployment_to_migrate,
                namespace=self.agent.namespace,
                body=body,
            )
            return True
        except Exception as e:
            print(f"[Execute] ❌ patch_deployment error: {e}")
            return False

    async def run(self):
        print("--- [Execute] ---")
        target_node = self.agent.knowledge.get("target_node")
        success = await asyncio.get_running_loop().run_in_executor(
            None, self.patch_deployment, target_node
        )
        if success:
            print(f"[Execute] ✅ Migración a '{target_node}' iniciada. Cooldown 45s...")
            await asyncio.sleep(45)
        else:
            print(f"[Execute] ❌ Falló la migración a '{target_node}'.")

        self.agent.migration_in_flight = False
        print("[Execute] Lock liberado.")


# --- Agent ---

class ClusterAgent(spade.agent.Agent):
    async def setup(self):
        self.knowledge              = {}
        self.migration_in_flight   = False
        self.current_node          = "r3-node"

        # Policy hot-reload state
        self.current_policy_filename = ""
        self.current_policy_mtime    = 0.0

        # Metrics hot-reload state
        self.active_metrics          = {}   # {name: {query, type, ...}}
        self.current_metrics_mtime   = 0.0

        # Cluster config
        self.prometheus_url          = "http://192.168.0.42:9090"
        self.namespace               = "smartcity"
        self.deployment_to_migrate   = "traffic-processor"
        self.pod_label_selector      = "app=traffic-processor"
        self.kube_context            = "clus1"
        self.kubeconfig_path         = os.path.join(SCRIPT_DIR, "kubeconfig")
        self.policy_conf_path        = os.path.join(SCRIPT_DIR, "policy.conf")
        self.metrics_conf_path       = os.path.join(SCRIPT_DIR, "metrics.conf")

        # Decision thresholds (escala [0.0–1.0] para QoS, % para CPU)
        self.qos_threshold           = 0.80
        self.return_cpu_threshold    = 50.0
        self.migration_cost          = 0.20
        self.node_costs              = {"r3-node": 0.0, "vm1node": 0.50}

        self.policy = PassivePolicy(self)
        print("[Agent] Iniciado con PassivePolicy por defecto.")
        self.add_behaviour(MonitorBehaviour(period=15))
        self.add_behaviour(AnalyzeBehaviour(
            start_at=datetime.datetime.now() + datetime.timedelta(seconds=5),
            period=15,
        ))


async def main():
    agent = ClusterAgent("cluster_agent@localhost", "123")
    await agent.start()
    print("Agent running. Press Ctrl+C to stop.")
    try:
        while agent.is_alive():
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        await agent.stop()
        print("Agent stopped.")

if __name__ == "__main__":
    asyncio.run(main())
