import spade
from spade.behaviour import PeriodicBehaviour, OneShotBehaviour
from spade.message import Message
import datetime
import httpx
import json
import logging
import asyncio

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
        # print("--- [Monitor] Running ---")
        
        # 1. Get real CPU usage for ALL nodes from Prometheus
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
                # if not nodes_cpu:
                #     print("[Monitor] CPU query OK, but no nodes were found in the results.")

        except Exception as e:
            print(f"[Monitor] ERROR fetching CPU: {e}")
            self.agent.knowledge["nodes_cpu"] = {}

        # 2. Get real iterations/second from Prometheus
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
                    # print(f"[Monitor] Iterations query returned no results.")
        except Exception as e:
            print(f"[Monitor] ERROR fetching iterations: {e}")
            self.agent.knowledge["iter"] = 0.0

        # nodes_cpu_log = {k: f"{v:.2f}%" for k, v in self.agent.knowledge.get('nodes_cpu', {}).items()}
        # iter_val = self.agent.knowledge.get('iter', 0.0)
        # print(f"[Monitor] Metrics updated: nodes_cpu(real)={nodes_cpu_log}  iter(real)={iter_val:.2f}")


class AnalyzeBehaviour(PeriodicBehaviour):
    async def run(self):
        nodes_cpu = self.agent.knowledge.get("nodes_cpu", {})
        cpu_to_check = nodes_cpu.get(self.agent.node_to_monitor, 0.0)
        itr = self.agent.knowledge.get("iter", 0.0)
        
        # print(f"[Analyze] Checking node '{self.agent.node_to_monitor}': cpu={cpu_to_check:.2f}%, iter={itr:.2f}")

        if self.agent.migration_in_flight:
            # print("[Analyze] Migration already in flight. Skipping analysis.")
            return

        # Hysteresis Logic: only trigger migration if degradation is sustained
        is_degraded = cpu_to_check > self.agent.cpu_threshold and itr > 0 and itr < (self.agent.baseline_iter * 0.8)

        if is_degraded:
            if self.agent.degradation_start_time is None:
                self.agent.degradation_start_time = datetime.datetime.now()
                # print(f"[Analyze] ⚠️ Degradation detected on '{self.agent.node_to_monitor}'. Starting timer...")
            else:
                duration = (datetime.datetime.now() - self.agent.degradation_start_time).total_seconds()
                # print(f"[Analyze] Degradation ongoing for {duration:.0f} seconds (threshold is {self.agent.degradation_time_threshold}s).")
                if duration >= self.agent.degradation_time_threshold:
                    print(f"[Analyze] ‼️ Degradation has been sustained for over {self.agent.degradation_time_threshold}s. Triggering Plan!")
                    self.agent.migration_in_flight = True
                    self.agent.degradation_start_time = None 
                    self.agent.add_behaviour(PlanBehaviour())
        else:
            if self.agent.degradation_start_time is not None:
                # print("[Analyze] ✅ System has recovered. Resetting degradation timer.")
                self.agent.degradation_start_time = None
            # else:
            #     print("[Analyze] ✅ System stable.")


class PlanBehaviour(OneShotBehaviour):
    async def run(self):
        print("[Plan] selecting target node...")
        target_node = "r3-node"
        print(f"[Plan] target node selected: {target_node}")
        self.agent.knowledge["target_node"] = target_node
        self.agent.add_behaviour(ExecuteBehaviour())


class ExecuteBehaviour(OneShotBehaviour):
    async def run(self):
        target = self.agent.knowledge.get("target_node", "unknown")
        print(f"[Execute] migrating pod → {target}")
        await asyncio.sleep(5)
        print(f"[Execute] ✅ pod migrated to {target}")
        self.agent.knowledge["current_node"] = target
        self.agent.migration_in_flight = False
        # print("[Execute] migration_in_flight released — back to monitoring")


class ClusterAgent(spade.agent.Agent):
    async def setup(self):
        # print(f"[Agent] starting — {str(self.jid)}")
        self.knowledge = {}
        self.migration_in_flight = False
        self.degradation_start_time = None # For hysteresis

        # --- Configuration ---
        self.prometheus_url = "http://192.168.0.35:9090"
        self.node_to_monitor = "vm1node"
        self.prometheus_iter_query = 'main_app_iterations_per_sec{service="main-app", cluster="clus1"}'
        self.prometheus_cpu_query = '100 * sum by (cluster, k8s_node_name) (rate(system_cpu_time_seconds_total{job="otel-collectors", state!="idle"}[1m])) / count by (cluster, k8s_node_name) (system_cpu_time_seconds_total{job="otel-collectors", state="idle"})'
        
        # --- Policy Thresholds ---
        self.cpu_threshold = 80.0
        self.baseline_iter = 1000.0
        self.degradation_time_threshold = 120 # seconds (2 minutes)

        self.add_behaviour(KeepAliveBehaviour(period=30))
        self.add_behaviour(MonitorBehaviour(period=15))
        start_analyze = datetime.datetime.now() + datetime.timedelta(seconds=5)
        self.add_behaviour(AnalyzeBehaviour(period=15, start_at=start_analyze))

        # print("[Agent] all behaviours added")


async def main():
    agent = ClusterAgent("cluster_agent@192.168.0.35", "123")
    await agent.start()
    print("[Agent] running — Ctrl+C to stop")
    await spade.wait_until_finished(agent)


if __name__ == "__main__":
    spade.run(main())
