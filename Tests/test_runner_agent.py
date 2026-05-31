"""
test_runner_agent.py
====================
Runs multiple policies (base, threshold, weights) in a single overnight session.

For each policy it:
  1. Writes policy.conf so the cluster agent hot-reloads it.
  2. Runs --num-tests tests, each with stressor apply/cleanup.
  3. Saves an individual JSON per test (fault-tolerant).
  4. Saves a per-policy summary JSON (written after every test, updated on failure).
  5. Saves/updates a master all_results.json after each policy block.

Usage:
  python test_runner_agent.py \\
    --policies base threshold weights \\
    --num-tests 3 \\
    --duration 3600 \\
    --node-ip 192.168.0.15 \\
    --policy-conf-path ../Cluster/policy.conf
"""

import argparse
import json
import time
import datetime
import os
import httpx
import asyncio
import subprocess
from kubernetes import client, config

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SCRIPT_DIR           = os.path.dirname(os.path.abspath(__file__))
K8S_NAMESPACE        = "smartcity"
POD_LABEL_SELECTOR   = "app=traffic-processor"
STRESSOR_POD_NAME    = "cpu-wave"
OPTIMAL_NODE         = "r3-node"
STRESSOR_YAML_PATH   = os.path.join(SCRIPT_DIR, "stressor.yaml")

# Mapping: policy name → filename to write in policy.conf
# "base" uses PassivePolicy (empty file)
POLICY_FILES = {
    "base":      "",            # empty → PassivePolicy
    "threshold": "threshold.py",
    "weights":   "weights.py",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def log(msg: str):
    ts = datetime.datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")


def run_kubectl_command(command, kube_context, input_yaml=None):
    full_command = ["kubectl", f"--context={kube_context}"] + command
    try:
        process = subprocess.run(
            full_command, input=input_yaml, text=True,
            capture_output=True, check=True
        )
        return process.stdout.strip()
    except subprocess.CalledProcessError as e:
        log(f"[ERROR] kubectl failed: {' '.join(full_command)}\n  {e.stderr.strip()}")
        return None


def get_pod_node(api_client):
    try:
        pod_list = api_client.list_namespaced_pod(
            namespace=K8S_NAMESPACE, label_selector=POD_LABEL_SELECTOR
        )
        if pod_list.items:
            return pod_list.items[0].spec.node_name
        return None
    except Exception as e:
        log(f"[ERROR] K8s API call failed: {e}")
        return None


async def query_prometheus(prometheus_url, query):
    try:
        async with httpx.AsyncClient(timeout=5.0) as c:
            r = await c.get(f"{prometheus_url}/api/v1/query", params={"query": query})
            r.raise_for_status()
            return r.json().get("data", {}).get("result", [])
    except Exception as e:
        log(f"[WARN] Prometheus query failed: {e}")
        return None


async def check_health(health_url):
    try:
        async with httpx.AsyncClient(timeout=1.0) as c:
            r = await c.get(health_url)
            return r.status_code == 200
    except (httpx.ConnectError, httpx.TimeoutException):
        return False


# ---------------------------------------------------------------------------
# Policy switching
# ---------------------------------------------------------------------------

def switch_policy(policy_name: str, policy_conf_path: str):
    """Write policy.conf so the cluster agent hot-reloads it."""
    filename = POLICY_FILES.get(policy_name, "")
    with open(policy_conf_path, "w") as f:
        f.write(filename)
    log(f"policy.conf → '{filename or '(empty=base)'}'. Waiting 20s for agent reload...")
    time.sleep(20)   # agent polls every 15s; 20s guarantees a reload cycle


# ---------------------------------------------------------------------------
# Stressor & pod placement
# ---------------------------------------------------------------------------

def ensure_pod_on_optimal_node(api_client, kube_context):
    log(f"Pre-check: ensuring pod is on '{OPTIMAL_NODE}'...")
    for _ in range(10):
        node = get_pod_node(api_client)
        if node == OPTIMAL_NODE:
            log(f"Pod is on '{OPTIMAL_NODE}'. OK.")
            return
        log(f"Pod on '{node}' → patching to '{OPTIMAL_NODE}'...")
        patch_cmd = [
            "patch", "deployment", "traffic-processor",
            "-n", K8S_NAMESPACE, "-p",
            f'{{"spec":{{"template":{{"spec":{{"nodeSelector":{{"kubernetes.io/hostname":"{OPTIMAL_NODE}"}}}}}}}}}}'
        ]
        run_kubectl_command(patch_cmd, kube_context)
        time.sleep(20)
    log("[WARN] Could not confirm pod placement after 10 attempts. Continuing anyway.")


def apply_stressor(kube_context):
    log(f"Applying stressor '{STRESSOR_POD_NAME}'...")
    run_kubectl_command(["apply", "-f", STRESSOR_YAML_PATH], kube_context)


def cleanup_stressor(kube_context):
    log(f"Cleaning up stressor '{STRESSOR_POD_NAME}'...")
    run_kubectl_command(["delete", "-f", STRESSOR_YAML_PATH, "--ignore-not-found=true"], kube_context)


# ---------------------------------------------------------------------------
# Single test measurement
# ---------------------------------------------------------------------------

async def run_single_test(test_number, policy_name, args, api_client) -> dict:
    start_obj = datetime.datetime.now()
    log(f"--- Test #{test_number} [{policy_name}] START {start_obj.isoformat()} ---")

    start_time  = time.time()
    end_time    = start_time + args.duration
    health_url  = f"http://{args.node_ip}:{args.node_port}/health"

    migration_count          = 0
    total_qos_violation_s    = 0.0
    total_downtime_s         = 0.0
    time_per_node            = {"r3-node": 0.0, "vm1node": 0.0}

    previous_node            = None
    qos_violation_start      = None
    downtime_start           = None
    interval_start           = start_time

    while time.time() < end_time:
        loop_start = time.time()

        current_node, qos_results, healthy = await asyncio.gather(
            asyncio.to_thread(get_pod_node, api_client),
            query_prometheus(args.prometheus_url, 'qos_composite{service=~"traffic-processor.*"}'),
            check_health(health_url),
        )

        # --- Node tracking ---
        if current_node:
            if previous_node is None:
                previous_node = current_node
                log(f"Initial node: '{current_node}'")
            if current_node != previous_node:
                log(f"MIGRATION: '{previous_node}' → '{current_node}'")
                migration_count += 1
                elapsed = loop_start - interval_start
                time_per_node[previous_node] = time_per_node.get(previous_node, 0.0) + elapsed
                previous_node  = current_node
                interval_start = loop_start

        # --- QoS violation tracking ---
        if qos_results:
            qos = float(qos_results[0]["value"][1])
            if qos < args.qos_threshold:
                if qos_violation_start is None:
                    qos_violation_start = loop_start
            elif qos_violation_start is not None:
                total_qos_violation_s += loop_start - qos_violation_start
                qos_violation_start = None

        # --- Downtime tracking ---
        if not healthy:
            if downtime_start is None:
                downtime_start = loop_start
        elif downtime_start is not None:
            total_downtime_s += loop_start - downtime_start
            downtime_start = None

        await asyncio.sleep(max(0.0, 1.0 - (time.time() - loop_start)))

    # --- Close open intervals ---
    now = time.time()
    if previous_node:
        time_per_node[previous_node] = time_per_node.get(previous_node, 0.0) + (now - interval_start)
    if qos_violation_start:
        total_qos_violation_s += now - qos_violation_start
    if downtime_start:
        total_downtime_s += now - downtime_start

    end_obj = datetime.datetime.now()
    log(f"--- Test #{test_number} [{policy_name}] END {end_obj.isoformat()} ---")

    return {
        "test_number":          test_number,
        "policy_name":          policy_name,
        "test_start_timestamp": start_obj.isoformat(),
        "test_end_timestamp":   end_obj.isoformat(),
        "test_duration_seconds": round(now - start_time),
        "metrics": {
            "migration_count":       migration_count,
            "time_on_r3_node_s":     round(time_per_node.get("r3-node", 0.0)),
            "time_on_vm1node_s":     round(time_per_node.get("vm1node", 0.0)),
            "qos_violation_seconds": round(total_qos_violation_s),
            "downtime_seconds":      round(total_downtime_s, 2),
        },
    }


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------

def save_json(path: str, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    log(f"Saved: {os.path.relpath(path, SCRIPT_DIR)}")


def save_policy_summary(run_dir: str, policy_name: str, tests: list, args):
    """Written after every individual test — always up to date."""
    summary = {
        "policy":     policy_name,
        "run_id":     os.path.basename(run_dir),
        "num_tests":  len(tests),
        "duration_per_test_s": args.duration,
        "qos_threshold": args.qos_threshold,
        "tests":      tests,
        "aggregated": aggregate(tests),
    }
    path = os.path.join(run_dir, f"{policy_name}_summary.json")
    save_json(path, summary)
    return summary


def save_all_results(run_dir: str, all_summaries: dict, run_meta: dict):
    """Master file — updated after each policy block."""
    master = {
        **run_meta,
        "policies_completed": list(all_summaries.keys()),
        "results": all_summaries,
    }
    save_json(os.path.join(run_dir, "all_results.json"), master)


def aggregate(tests: list) -> dict:
    """Simple averages over a list of test result dicts."""
    if not tests:
        return {}
    keys = ["migration_count", "time_on_r3_node_s", "time_on_vm1node_s",
            "qos_violation_seconds", "downtime_seconds"]
    agg = {}
    for k in keys:
        vals = [t["metrics"].get(k, 0) for t in tests]
        agg[f"avg_{k}"] = round(sum(vals) / len(vals), 2)
        agg[f"total_{k}"] = round(sum(vals), 2)
    return agg


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    parser = argparse.ArgumentParser(description="Multi-policy overnight test runner.")
    parser.add_argument("--policies",         nargs="+", default=["base", "threshold", "weights"],
                        help="Policies to test in order (default: base threshold weights)")
    parser.add_argument("--num-tests",        type=int,   default=3)
    parser.add_argument("--duration",         type=int,   default=3600,
                        help="Duration of each test in seconds")
    parser.add_argument("--cooldown-tests",   type=int,   default=300,
                        help="Cooldown between tests in seconds (default: 5 min)")
    parser.add_argument("--cooldown-policies",type=int,   default=120,
                        help="Cooldown between policies in seconds (default: 2 min)")
    parser.add_argument("--kube-context",     type=str,   default="clus1")
    parser.add_argument("--kubeconfig-path",  type=str,   default="../Cluster/kubeconfig")
    parser.add_argument("--prometheus-url",   type=str,   default="http://192.168.0.42:9090")
    parser.add_argument("--qos-threshold",    type=float, default=0.80)
    parser.add_argument("--node-ip",          type=str,   required=True)
    parser.add_argument("--node-port",        type=int,   default=30080)
    parser.add_argument("--policy-conf-path", type=str,
                        default=os.path.join(SCRIPT_DIR, "../Cluster/policy.conf"))
    args = parser.parse_args()

    # Validate policies
    for p in args.policies:
        if p not in POLICY_FILES:
            print(f"[FATAL] Unknown policy '{p}'. Valid: {list(POLICY_FILES)}")
            return

    # Create run directory
    run_id  = datetime.datetime.now().strftime("run_%Y%m%d_%H%M%S")
    run_dir = os.path.join(SCRIPT_DIR, "results", run_id)
    os.makedirs(run_dir, exist_ok=True)

    run_meta = {
        "run_id":              run_id,
        "run_start":           datetime.datetime.now().isoformat(),
        "policies":            args.policies,
        "num_tests_per_policy":args.num_tests,
        "duration_per_test_s": args.duration,
        "qos_threshold":       args.qos_threshold,
        "node_ip":             args.node_ip,
    }
    save_json(os.path.join(run_dir, "manifest.json"), run_meta)

    # Load kubeconfig
    try:
        config.load_kube_config(config_file=args.kubeconfig_path, context=args.kube_context)
        v1 = client.CoreV1Api()
    except Exception as e:
        log(f"[FATAL] Could not load kubeconfig: {e}")
        return

    policy_conf_path = os.path.abspath(args.policy_conf_path)
    all_summaries    = {}
    total_policies   = len(args.policies)

    for p_idx, policy_name in enumerate(args.policies, start=1):
        log(f"\n{'='*60}")
        log(f"POLICY {p_idx}/{total_policies}: '{policy_name.upper()}'")
        log(f"{'='*60}")

        # Switch policy
        switch_policy(policy_name, policy_conf_path)

        policy_tests = []

        for i in range(1, args.num_tests + 1):
            log(f"\n--- Test {i}/{args.num_tests} [{policy_name}] ---")

            # Setup
            cleanup_stressor(args.kube_context)
            ensure_pod_on_optimal_node(v1, args.kube_context)
            apply_stressor(args.kube_context)
            log("Waiting 15s for stressor to stabilize...")
            time.sleep(15)

            # Run
            try:
                result = await run_single_test(i, policy_name, args, v1)
            except Exception as e:
                log(f"[ERROR] Test {i} failed with exception: {e}")
                result = {
                    "test_number": i,
                    "policy_name": policy_name,
                    "error": str(e),
                }

            # Save individual test JSON immediately
            ts   = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            name = f"test_{i}_{policy_name}_{ts}.json"
            save_json(os.path.join(run_dir, name), result)

            policy_tests.append(result)

            # Update per-policy summary after every test
            summary = save_policy_summary(run_dir, policy_name, policy_tests, args)

            # Update master file after every test
            all_summaries[policy_name] = summary
            save_all_results(run_dir, all_summaries, run_meta)

            # Cleanup + cooldown between tests
            cleanup_stressor(args.kube_context)
            if i < args.num_tests:
                log(f"Cooldown {args.cooldown_tests}s before next test...")
                time.sleep(args.cooldown_tests)

        log(f"\nPolicy '{policy_name}' complete. {len(policy_tests)} tests done.")

        # Cooldown between policies (reset to base before next policy)
        if p_idx < total_policies:
            log("Resetting to base policy during inter-policy cooldown...")
            switch_policy("base", policy_conf_path)
            log(f"Inter-policy cooldown {args.cooldown_policies}s...")
            time.sleep(args.cooldown_policies)

    # Final: restore base policy
    switch_policy("base", policy_conf_path)

    run_meta["run_end"] = datetime.datetime.now().isoformat()
    save_all_results(run_dir, all_summaries, run_meta)

    log(f"\n{'='*60}")
    log(f"ALL DONE. Results in: Tests/results/{run_id}/")
    log(f"  - manifest.json")
    for p in args.policies:
        log(f"  - {p}_summary.json")
    log(f"  - all_results.json")
    log(f"{'='*60}")


if __name__ == "__main__":
    asyncio.run(main())
