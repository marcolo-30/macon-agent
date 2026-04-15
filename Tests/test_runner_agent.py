import argparse
import json
import time
import datetime
import os
import httpx
import asyncio
import subprocess
from kubernetes import client, config

# --- Configuration ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
K8S_NAMESPACE = "myapp"
POD_LABEL_SELECTOR = "app=main-app"
STRESSOR_POD_NAME = "cpu-wave"
OPTIMAL_NODE = "r3-node"
STRESSOR_YAML_PATH = os.path.join(SCRIPT_DIR, "stressor.yaml")

# --- Helper Functions ---

def run_kubectl_command(command, kube_context, input_yaml=None):
    full_command = ["kubectl", f"--context={kube_context}"] + command
    try:
        process = subprocess.run(full_command, input=input_yaml, text=True, capture_output=True, check=True)
        return process.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] kubectl command failed: {' '.join(full_command)}\nStderr: {e.stderr}")
        return None

def get_pod_node(api_client):
    try:
        pod_list = api_client.list_namespaced_pod(namespace=K8S_NAMESPACE, label_selector=POD_LABEL_SELECTOR)
        if pod_list.items:
            return pod_list.items[0].spec.node_name
        return None
    except Exception as e:
        print(f"[ERROR] K8s API call for pod node failed: {e}")
        return None

async def query_prometheus(prometheus_url, query):
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{prometheus_url}/api/v1/query", params={'query': query})
            response.raise_for_status()
            return response.json().get('data', {}).get('result', [])
    except Exception as e:
        print(f"[ERROR] Prometheus query failed for query '{query}': {e}")
        return None

async def check_health(health_url):
    try:
        async with httpx.AsyncClient(timeout=1.0) as client:
            response = await client.get(health_url)
            return response.status_code == 200
    except (httpx.ConnectError, httpx.TimeoutException):
        return False

# --- Test Orchestration ---

def ensure_pod_location(api_client, target_node, kube_context):
    print(f"--- Pre-condition: Ensuring pod is on '{target_node}' ---")
    while True:
        current_node = get_pod_node(api_client)
        if current_node == target_node:
            print(f"[INFO] Pod is correctly located on '{target_node}'.")
            break
        print(f"[WARN] Pod is on '{current_node}', moving it to '{target_node}'...")
        patch_cmd = ["patch", "deployment", "main-app", "-n", K8S_NAMESPACE, "-p", f'{{"spec":{{"template":{{"spec":{{"nodeSelector":{{"kubernetes.io/hostname":"{target_node}"}}}}}}}}}}']
        run_kubectl_command(patch_cmd, kube_context)
        time.sleep(15)
    print("--- Pre-condition MET ---")

def apply_stressor(kube_context):
    print(f"--- Applying stressor pod from '{STRESSOR_YAML_PATH}' ---")
    run_kubectl_command(["apply", "-f", STRESSOR_YAML_PATH], kube_context)
    print("--- Stressor applied ---")

def cleanup_stressor(kube_context):
    print(f"--- Cleaning up stressor pod '{STRESSOR_POD_NAME}' ---")
    run_kubectl_command(["delete", "-f", STRESSOR_YAML_PATH, "--ignore-not-found=true"], kube_context)
    print("--- Cleanup complete ---")

async def run_single_test(test_number, args, api_client):
    start_time_obj = datetime.datetime.now()
    print(f"--- Starting Measurement for Test #{test_number}, Start Time: {start_time_obj.isoformat()} ---")

    start_time = time.time()
    end_time = start_time + args.duration
    health_check_url = f"http://{args.node_ip}:{args.node_port}/health"

    migration_count, total_qos_violation_seconds, total_downtime_seconds = 0, 0, 0
    time_per_node = {"r3-node": 0, "vm1node": 0}
    previous_node, qos_violation_start_time, downtime_start_time = None, None, None
    interval_start_time = start_time

    while time.time() < end_time:
        loop_start_time = time.time()

        current_node, qos_results, is_healthy = await asyncio.gather(
            asyncio.to_thread(get_pod_node, api_client),
            query_prometheus(args.prometheus_url, 'main_app_qos{service="main-app", cluster="clus1"}'),
            check_health(health_check_url)
        )

        if current_node:
            if previous_node is None:
                previous_node = current_node
                print(f"[INFO] Initial pod location detected: '{current_node}'")
            if current_node != previous_node:
                print(f"[MIGRATION DETECTED] Pod moved from '{previous_node}' to '{current_node}'")
                migration_count += 1
                elapsed = loop_start_time - interval_start_time
                if previous_node in time_per_node: time_per_node[previous_node] += elapsed
                previous_node = current_node
                interval_start_time = loop_start_time

        if qos_results:
            current_qos = float(qos_results[0]['value'][1]) if qos_results else 100.0
            if current_qos < args.qos_threshold:
                if qos_violation_start_time is None: qos_violation_start_time = loop_start_time
            elif qos_violation_start_time is not None:
                total_qos_violation_seconds += loop_start_time - qos_violation_start_time
                qos_violation_start_time = None
        
        if not is_healthy:
            if downtime_start_time is None: downtime_start_time = loop_start_time
        elif downtime_start_time is not None:
            total_downtime_seconds += loop_start_time - downtime_start_time
            downtime_start_time = None

        await asyncio.sleep(max(0, 1 - (time.time() - loop_start_time)))

    end_time_obj = datetime.datetime.now()
    if previous_node in time_per_node: time_per_node[previous_node] += time.time() - interval_start_time
    if qos_violation_start_time: total_qos_violation_seconds += time.time() - qos_violation_start_time
    if downtime_start_time: total_downtime_seconds += time.time() - downtime_start_time

    print(f"--- Measurement Finished at {end_time_obj.isoformat()} ---")
    
    result = {
        "test_number": test_number,
        "policy_name": args.policy,
        "test_start_timestamp": start_time_obj.isoformat(),
        "test_end_timestamp": end_time_obj.isoformat(),
        "test_duration_seconds": round(time.time() - start_time),
        "metrics": {
            "migration_count": migration_count,
            "time_on_r3_node_seconds": round(time_per_node.get("r3-node", 0)),
            "time_on_vm1node_seconds": round(time_per_node.get("vm1node", 0)),
            "qos_violation_seconds": round(total_qos_violation_seconds),
            "downtime_seconds": round(total_downtime_seconds, 2)
        }
    }
    print(f"Results: {json.dumps(result, indent=2)}")
    return result

async def main():
    parser = argparse.ArgumentParser(description="Run a series of MAPE-K tests.")
    parser.add_argument("--policy", type=str, required=True)
    parser.add_argument("--num-tests", type=int, default=10)
    parser.add_argument("--duration", type=int, default=3600)
    parser.add_argument("--kube-context", type=str, default="clus1")
    parser.add_argument("--kubeconfig-path", type=str, default="../Cluster/kubeconfig")
    parser.add_argument("--prometheus-url", type=str, default="http://192.168.0.35:9090")
    parser.add_argument("--qos-threshold", type=float, default=80.0)
    parser.add_argument("--node-ip", type=str, required=True, help="IP of a k8s node to reach the NodePort service.")
    parser.add_argument("--node-port", type=int, default=30007, help="NodePort of the main-app health check service.")
    args = parser.parse_args()

    results_dir = os.path.join(SCRIPT_DIR, "results")
    os.makedirs(results_dir, exist_ok=True)
    
    try:
        config.load_kube_config(config_file=args.kubeconfig_path, context=args.kube_context)
        v1 = client.CoreV1Api()
    except Exception as e:
        print(f"[FATAL] Could not load kubeconfig. Aborting. Error: {e}")
        return

    for i in range(1, args.num_tests + 1):
        print(f"\n========= Starting Test Run {i}/{args.num_tests} for Policy '{args.policy}' =========")
        cleanup_stressor(args.kube_context)
        ensure_pod_location(v1, OPTIMAL_NODE, args.kube_context)
        apply_stressor(args.kube_context)
        print("[INFO] Waiting 15s for stressor to stabilize...")
        time.sleep(15)

        test_result = await run_single_test(i, args, v1)
        
        if test_result:
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            file_name = f"test_{i}_{args.policy}_{timestamp}.json"
            output_path = os.path.join(results_dir, file_name)
            with open(output_path, 'w') as f:
                json.dump(test_result, f, indent=2)
            print(f"Results for test {i} saved to {output_path}")

        cleanup_stressor(args.kube_context)
        print(f"========= Finished Test Run {i}/{args.num_tests}. Cooldown 5 minutes. =========")
        if i < args.num_tests: time.sleep(300)
    
    print(f"\n\nAll test runs complete.")

if __name__ == "__main__":
    asyncio.run(main())
