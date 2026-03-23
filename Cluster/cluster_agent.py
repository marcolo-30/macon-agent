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
        # print("[KeepAlive] Ping sent to keep connection alive.")


class MonitorBehaviour(PeriodicBehaviour):
    async def run(self):
        print("--- [Monitor] Running ---")
        
        try:
            url = f"{self.agent.prometheus_url}/api/v1/query?query={self.agent.prometheus_cpu_query}"
            async with httpx.AsyncClient() as client:
                response = await client.get(url)
                response.raise_for_status()
                data = response.json()
                results = data.get('data', {}).get('result', [])
                
                nodes_cpu = {}
                for result in results:
                    node_name = result.get('metric', {}).get('k8s_node_name')
                    if node_name:
                        cpu_usage = float(result['value'][1])
                        nodes_cpu[node_name] = cpu_usage
                self.agent.knowledge["nodes_cpu"] = nodes_cpu
        except Exception as e:
            print(f"[Monitor] ERROR fetching CPU: {e}")
            self.agent.knowledge["nodes_cpu"] = {}

        try:
            url = f"{self.agent.prometheus_url}/api/v1/query?query={self.agent.prometheus_iter_query}"
            async with httpx.AsyncClient() as client:
                response = await client.get(url)
                response.raise_for_status()
                data = response.json()
                result = data.get('data', {}).get('result', [])
                if result:
                    self.agent.knowledge["iter"] = float(result[0]['value'][1])
                else:
                    self.agent.knowledge["iter"] = 0.0
        except Exception as e:
            print(f"[Monitor] ERROR fetching iterations: {e}")
            self.agent.knowledge["iter"] = 0.0

        nodes_cpu_log = {k: f"{v:.2f}%" for k, v in self.agent.knowledge.get('nodes_cpu', {}).items()}
        iter_val = self.agent.knowledge.get('iter', 0.0)
        print(f"[Monitor] Metrics updated: nodes_cpu(real)={nodes_cpu_log}  iter(real)={iter_val:.2f}")


class AnalyzeBehaviour(PeriodicBehaviour):
    async def run(self):
        nodes_cpu = self.agent.knowledge.get("nodes_cpu", {})
        cpu_to_check = nodes_cpu.get(self.agent.node_to_monitor, 0.0)
        itr = self.agent.knowledge.get("iter", 0.0)
        
        print(f"[Analyze] Checking node '{self.agent.node_to_monitor}': cpu={cpu_to_check:.2f}%, iter={itr:.2f}")

        if self.agent.migration_in_flight:
            print("[Analyze] Migration already in flight. Skipping analysis.")
            return

        is_degraded = cpu_to_check > self.agent.cpu_threshold and itr > 0 and itr < (self.agent.baseline_iter * 0.8)

        if is_degraded:
            if self.agent.degradation_start_time is None:
                self.agent.degradation_start_time = datetime.datetime.now()
                print(f"[Analyze] ⚠️ Degradation detected on '{self.agent.node_to_monitor}'. Starting timer...")
            else:
                duration = (datetime.datetime.now() - self.agent.degradation_start_time).total_seconds()
                print(f"[Analyze] Degradation ongoing for {duration:.0f} seconds (threshold is {self.agent.degradation_time_threshold}s).")
                if duration >= self.agent.degradation_time_threshold:
                    print(f"[Analyze] ‼️ Degradation has been sustained for over {self.agent.degradation_time_threshold}s. Triggering Plan!")
                    self.agent.migration_in_flight = True
                    self.agent.degradation_start_time = None 
                    self.agent.add_behaviour(PlanBehaviour())
        else:
            if self.agent.degradation_start_time is not None:
                print("[Analyze] ✅ System has recovered. Resetting degradation timer.")
                self.agent.degradation_start_time = None
            else:
                print("[Analyze] ✅ System stable.")


class PlanBehaviour(OneShotBehaviour):
    async def run(self):
        print("[Plan] Selecting target node...")
        target_node = "r3-node"
        print(f"[Plan] Target node selected: {target_node}")
        self.agent.knowledge["target_node"] = target_node
        self.agent.add_behaviour(ExecuteBehaviour())


class ExecuteBehaviour(OneShotBehaviour):
    
    def patch_deployment(self, target_node):
        """
        This is a blocking function that patches the deployment.
        It will be run in a separate thread to not block the agent's event loop.
        """
        try:
            # Load Kubernetes configuration from default location, specifying the context
            config.load_kube_config(context=self.agent.kube_context)
            
            # Create an API client
            api_client = client.AppsV1Api()

            # Define the patch body
            body = {
                "spec": {
                    "template": {
                        "spec": {
                            "nodeSelector": {
                                "kubernetes.io/hostname": target_node
                            }
                        }
                    }
                }
            }

            # Patch the deployment
            api_response = api_client.patch_namespaced_deployment(
                name=self.agent.deployment_to_migrate,
                namespace=self.agent.namespace,
                body=body
            )
            
            print(f"[Execute] ✅ SUCCESS: API call to patch deployment '{self.agent.deployment_to_migrate}' was successful.")
            return True, api_response.metadata.name
        
        except Exception as e:
            print(f"[Execute] ❌ ERROR: An exception occurred when calling Kubernetes API: {e}")
            return False, str(e)

    async def run(self):
        target_node = self.agent.knowledge.get("target_node", "unknown")
        print(f"[Execute] Attempting to migrate deployment '{self.agent.deployment_to_migrate}' to node '{target_node}' via Kubernetes API...")

        loop = asyncio.get_running_loop()
        
        # Run the blocking Kubernetes API call in a separate thread
        success, result = await loop.run_in_executor(
            None, self.patch_deployment, target_node
        )

        if success:
            self.agent.knowledge["current_node"] = target_node
        
        # IMPORTANT: Release the migration flag regardless of success or failure
        self.agent.migration_in_flight = False
        print("[Execute] Migration flag released. Returning to normal monitoring.")


class ClusterAgent(spade.agent.Agent):
    async def setup(self):
        print(f"[Agent] starting — {str(self.jid)}")
        self.knowledge = {}
        self.migration_in_flight = False
        self.degradation_start_time = None

        # --- Configuration ---
        self.prometheus_url = "http://192.168.0.35:9090"
        self.node_to_monitor = "vm1node"
        self.namespace = "myapp"
        self.deployment_to_migrate = "main-app"
        self.kube_context = "clus1" # Specify the kubeconfig context to use
        
        self.prometheus_iter_query = 'main_app_iterations_per_sec{service="main-app", cluster="clus1"}'
        self.prometheus_cpu_query = '100 * sum by (cluster, k8s_node_name) (rate(system_cpu_time_seconds_total{job="otel-collectors", state!="idle"}[1m])) / count by (cluster, k8s_node_name) (system_cpu_time_seconds_total{job="otel-collectors", state="idle"})'
        
        # --- Policy Thresholds ---
        self.cpu_threshold = 80.0
        self.baseline_iter = 1000.0
        self.degradation_time_threshold = 120

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
    # Make sure to install the kubernetes client: pip install kubernetes
    spade.run(main())
