import spade
from spade.behaviour import PeriodicBehaviour, OneShotBehaviour
from spade.message import Message
import datetime
import httpx
import json
import logging
import asyncio
from kubernetes import client, config

# --- Logger Setup ---
log = logging.getLogger("cluster_agent")
logging.basicConfig(level=logging.INFO, format="[%(asctime)s][%(levelname)s] %(message)s")


class KeepAliveBehaviour(PeriodicBehaviour):
    async def run(self):
        msg = Message(to="cluster_agent@192.168.0.35")
        msg.body = "ping"
        await self.send(msg)


class MonitorBehaviour(PeriodicBehaviour):

    def get_pod_node(self):
        try:
            v1 = client.CoreV1Api()
            pod_list = v1.list_namespaced_pod(namespace=self.agent.namespace, label_selector=self.agent.pod_label_selector)
            if pod_list.items:
                return pod_list.items[0].spec.node_name
            return None
        except Exception:
            return None

    async def run(self):
        print("--- [Monitor] Running ---")
        
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(None, config.load_kube_config, self.agent.kubeconfig_path, self.agent.kube_context)
            actual_node = await loop.run_in_executor(None, self.get_pod_node)
        except Exception as e:
            print(f"[Monitor] ❌ FATAL: Could not load kubeconfig. Error: {e}")
            return

        if actual_node:
            if self.agent.current_node != actual_node:
                print(f"[Monitor] 🧠 Belief updated: Pod is on '{actual_node}'.")
                self.agent.current_node = actual_node
        else:
            print("[Monitor] Could not verify pod location. Skipping.")
            return

        try:
            async with httpx.AsyncClient() as client:
                tasks = [
                    client.get(f"{self.agent.prometheus_url}/api/v1/query?query={self.agent.prometheus_cpu_query}"),
                    client.get(f"{self.agent.prometheus_url}/api/v1/query?query={self.agent.prometheus_qos_query}"),
                ]
                responses = await asyncio.gather(*tasks, return_exceptions=True)

            if not isinstance(responses[0], Exception):
                results = responses[0].json().get('data', {}).get('result', [])
                self.agent.knowledge["nodes_cpu"] = {res.get('metric', {}).get('k8s_node_name'): float(res['value'][1]) for res in results if res.get('metric', {}).get('k8s_node_name')}
            else: self.agent.knowledge["nodes_cpu"] = {}

            if not isinstance(responses[1], Exception):
                result = responses[1].json().get('data', {}).get('result', [])
                self.agent.knowledge["qos"] = float(result[0]['value'][1]) if result else 0.0
            else: self.agent.knowledge["qos"] = 0.0
        except Exception as e:
            print(f"[Monitor] General ERROR during metrics fetching: {e}")

        nodes_cpu_log = {k: f"{v:.2f}%" for k, v in self.agent.knowledge.get('nodes_cpu', {}).items()}
        qos_val = self.agent.knowledge.get('qos', 0.0)
        print(f"[Monitor] Metrics updated: nodes_cpu={nodes_cpu_log}, qos={qos_val:.2f}")


class AnalyzeBehaviour(PeriodicBehaviour):
    
    def trigger_migration(self, target_node, reason):
        print(f"[Analyze] ‼️ {reason} Triggering migration from '{self.agent.current_node}' to '{target_node}'!")
        self.agent.migration_in_flight = True
        self.agent.knowledge["target_node"] = target_node
        self.agent.add_behaviour(PlanBehaviour())

    async def run(self):
        if self.agent.migration_in_flight or self.agent.current_node == "unknown":
            return

        nodes_cpu = self.agent.knowledge.get("nodes_cpu", {})
        current_qos = self.agent.knowledge.get("qos", 0.0)
        
        print(f"[Analyze] Current state: app is on '{self.agent.current_node}'. QoS is {current_qos:.2f} (Threshold: {self.agent.qos_threshold}%)")

        # If app is on the optimal node (r3-node), check if QoS has dropped
        if self.agent.current_node == "r3-node":
            if current_qos < self.agent.qos_threshold:
                self.trigger_migration(target_node="vm1node", reason=f"QoS dropped below {self.agent.qos_threshold}%")
        
        # If app is on the backup node (vm1node), check if it's safe to return
        elif self.agent.current_node == "vm1node":
            cpu_r3_node = nodes_cpu.get("r3-node", 100.0)
            print(f"[Analyze] Checking return condition. CPU of 'r3-node' is {cpu_r3_node:.2f}% (Threshold: {self.agent.return_cpu_threshold}%)")
            if cpu_r3_node < self.agent.return_cpu_threshold:
                self.trigger_migration(target_node="r3-node", reason=f"Optimal node 'r3-node' is free.")


class PlanBehaviour(OneShotBehaviour):
    async def run(self):
        target_node = self.agent.knowledge.get("target_node", "unknown")
        print(f"[Plan] Plan confirmed: migrate to '{target_node}'.")
        self.agent.add_behaviour(ExecuteBehaviour())


class ExecuteBehaviour(OneShotBehaviour):
    def patch_deployment(self, target_node):
        try:
            config.load_kube_config(config_file=self.agent.kubeconfig_path, context=self.agent.kube_context)
            api_client = client.AppsV1Api()
            body = {"spec": {"template": {"spec": {"nodeSelector": {"kubernetes.io/hostname": target_node}}}}}
            api_client.patch_namespaced_deployment(name=self.agent.deployment_to_migrate, namespace=self.agent.namespace, body=body)
            return True
        except Exception as e:
            print(f"[Execute] ❌ ERROR: An exception occurred when calling Kubernetes API: {e}")
            return False

    async def run(self):
        target_node = self.agent.knowledge.get("target_node", "unknown")
        print(f"[Execute] Executing migration to node '{target_node}' via Kubernetes API...")
        loop = asyncio.get_running_loop()
        success = await loop.run_in_executor(None, self.patch_deployment, target_node)
        if success:
            print(f"[Execute] ✅ SUCCESS: API call to patch deployment seems successful.")
        self.agent.migration_in_flight = False
        print("[Execute] Migration flag released.")


class ClusterAgent(spade.agent.Agent):
    async def setup(self):
        self.knowledge = {}
        self.migration_in_flight = False
        self.current_node = "r3-node" # Assume it starts on the optimal node

        # --- Configuration ---
        self.prometheus_url = "http://192.168.0.35:9090"
        self.namespace = "myapp"
        self.deployment_to_migrate = "main-app"
        self.pod_label_selector = "app=main-app"
        self.kube_context = "clus1"
        self.kubeconfig_path = "C:/Users/Maloa/PycharmProjects/UNICAL_Agents/Cluster/kubeconfig"
        
        self.prometheus_cpu_query = '100 * sum by (cluster, k8s_node_name) (rate(system_cpu_time_seconds_total{job="otel-collectors", state!="idle"}[1m])) / count by (cluster, k8s_node_name) (system_cpu_time_seconds_total{job="otel-collectors", state="idle"})'
        self.prometheus_qos_query = 'main_app_qos{service="main-app", cluster="clus1"}'
        
        # --- Policy ---
        self.qos_threshold = 80.0 # If QoS on r3-node drops below this, migrate
        self.return_cpu_threshold = 50.0 # If r3-node CPU is below this, it's safe to return

        self.add_behaviour(KeepAliveBehaviour(period=30))
        self.add_behaviour(MonitorBehaviour(period=15))
        start_analyze = datetime.datetime.now() + datetime.timedelta(seconds=5)
        self.add_behaviour(AnalyzeBehaviour(period=15, start_at=start_analyze))
        print("[Agent] all behaviours added")


async def main():
    agent = ClusterAgent("cluster_agent@192.168.0.35", "123")
    await agent.start()
    print("[Agent] running — Ctrl+C to stop")
    await spade.wait_until_finished(agent)


if __name__ == "__main__":
    spade.run(main())
