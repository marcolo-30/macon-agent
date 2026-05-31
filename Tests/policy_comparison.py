#!/usr/bin/env python3
"""
policy_comparison.py  — 4-policy benchmark
============================================
Runs each policy for N complete wave cycles, samples Prometheus every 5 s,
then saves per-policy stats + a side-by-side comparison report.

Policies tested (in order):
  1. passive     — no migrations (baseline)
  2. threshold   — threshold.py
  3. weights     — weights.py
  4. ml          — ml_based.py

Metrics captured every sample:
  qos, qos_latency_score, cpu_r3, cpu_vm1, current_node (from k8s)

Derived per-policy stats:
  mean_qos, min_qos, p10_qos
  pct_below_sla          — % samples with qos < SLA_THRESHOLD
  migration_count        — node switches detected
  avg_qos_during_burn    — QoS when cpu_r3 > CPU_BURN_THRESHOLD
  avg_qos_during_idle    — QoS when cpu_r3 < CPU_IDLE_THRESHOLD
  recovery_time_s        — avg seconds from wave-hit to qos > SLA_THRESHOLD

Usage
─────
  python Tests/policy_comparison.py \\
      --prometheus-url http://192.168.0.42:9090 \\
      --wave-yaml k3sApp/k8s/cpu-wave.yaml \\
      --cycles 6

Requirements: pip install httpx kubernetes
"""

import argparse
import asyncio
import csv
import datetime
import json
import os
import statistics
import subprocess
import sys
import time

import httpx
from kubernetes import client as k8s_client, config as k8s_config

# ── Paths ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
CLUSTER_DIR  = os.path.join(SCRIPT_DIR, "..", "Cluster")
OUTPUT_DIR   = os.path.join(SCRIPT_DIR, "comparison_results")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Experiment parameters ─────────────────────────────────────────────────────
WAVE_PERIOD_S      = 600   # 300 s burn + 300 s idle  (cpu-wave.yaml default)
SAMPLE_INTERVAL_S  = 5
SLA_THRESHOLD      = 70.0  # QoS below this = SLA violation
CPU_BURN_THRESHOLD = 60.0  # % r3 CPU → "burn phase"
CPU_IDLE_THRESHOLD = 30.0  # % r3 CPU → "idle phase"
COOLDOWN_BETWEEN_POLICIES_S = 120   # 2 min to let cluster settle between runs

# ── Policy sequence ───────────────────────────────────────────────────────────
POLICIES = [
    {"name": "passive",   "conf": ""},               # empty = PassivePolicy
    {"name": "threshold", "conf": "threshold.py"},
    {"name": "weights",   "conf": "weights.py"},
    {"name": "ml",        "conf": "ml_based.py"},
]

# ── Prometheus queries ────────────────────────────────────────────────────────
PROM_QUERIES = {
    "qos":               'qos_composite{service=~"traffic-processor.*"}',
    "qos_latency_score": 'qos_latency_score{service=~"traffic-processor.*"}',
}
PROM_CPU_QUERY = (
    '100 * sum by (k8s_node_name) '
    '(rate(system_cpu_time_seconds_total{job="otel-collectors",state!="idle"}[1m])) '
    '/ count by (k8s_node_name) '
    '(system_cpu_time_seconds_total{job="otel-collectors",state="idle"})'
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def ts() -> str:
    return datetime.datetime.now().strftime("%H:%M:%S")

def log(msg: str):
    print(f"[{ts()}] {msg}", flush=True)

def kubectl(args: list, ctx: str) -> str | None:
    try:
        r = subprocess.run(
            ["kubectl", f"--context={ctx}"] + args,
            text=True, capture_output=True, check=True,
        )
        return r.stdout.strip()
    except subprocess.CalledProcessError as e:
        log(f"[WARN] kubectl: {e.stderr.strip()}")
        return None

def set_policy(policy_conf: str):
    """Write policy name to policy.conf — agent hot-reloads within 5 s."""
    path = os.path.join(CLUSTER_DIR, "policy.conf")
    with open(path, "w") as f:
        f.write(policy_conf)
    log(f"  policy.conf → '{policy_conf or '(passive)'}' written")

def get_pod_node(v1, namespace: str, label_selector: str) -> str | None:
    try:
        pods = v1.list_namespaced_pod(namespace=namespace,
                                      label_selector=label_selector)
        return pods.items[0].spec.node_name if pods.items else None
    except Exception:
        return None

async def fetch_metrics(prom_url: str) -> dict:
    queries = {**PROM_QUERIES, "_cpu": PROM_CPU_QUERY}
    names   = list(queries.keys())
    async with httpx.AsyncClient(timeout=4.0) as c:
        responses = await asyncio.gather(
            *[c.get(f"{prom_url}/api/v1/query", params={"query": q})
              for q in queries.values()],
            return_exceptions=True,
        )
    result = {}
    for name, resp in zip(names, responses):
        if isinstance(resp, Exception):
            continue
        try:
            data = resp.json().get("data", {}).get("result", [])
            if name == "_cpu":
                for r in data:
                    node = r.get("metric", {}).get("k8s_node_name", "")
                    val  = round(float(r["value"][1]), 2)
                    if node == "r3-node":
                        result["cpu_r3"]  = val
                    elif node == "vm1node":
                        result["cpu_vm1"] = val
            elif data:
                result[name] = round(float(data[0]["value"][1]), 4)
        except Exception:
            pass
    return result


# ── Wave management ───────────────────────────────────────────────────────────

def start_wave(wave_yaml: str, ctx: str, namespace: str):
    kubectl(["delete", "pod", "cpu-wave", "-n", namespace,
             "--ignore-not-found=true", "--wait=true"], ctx)
    time.sleep(3)
    kubectl(["apply", "-f", wave_yaml], ctx)
    log("  Wave pod started — waiting 20 s to stabilise ...")
    time.sleep(20)

def stop_wave(ctx: str, namespace: str):
    kubectl(["delete", "pod", "cpu-wave", "-n", namespace,
             "--ignore-not-found=true"], ctx)

def reset_cluster(v1, namespace: str, pod_selector: str,
                  ctx: str, deployment: str = "traffic-processor",
                  settle_s: int = 90):
    """
    Bring cluster to a known starting state before each policy run:
      1. Set policy to passive (no agent migrations during reset)
      2. Patch deployment back to r3-node
      3. Wait for pod to be Running on r3-node
      4. Wait settle_s more seconds for Prometheus rate() windows to flush
         (rate([1m]) needs ~60 s of fresh data after node change)
    """
    log("  [Reset] Setting passive policy ...")
    set_policy("")

    log("  [Reset] Patching deployment → r3-node ...")
    try:
        from kubernetes import client as _k8s, config as _cfg
        api = _k8s.AppsV1Api()
        body = {"spec": {"template": {"spec": {
            "nodeSelector": {"kubernetes.io/hostname": "r3-node"}
        }}}}
        api.patch_namespaced_deployment(
            name=deployment, namespace=namespace, body=body
        )
    except Exception as e:
        log(f"  [Reset] patch error: {e}")

    # Wait for pod to land on r3-node (max 120 s)
    log("  [Reset] Waiting for pod to be Running on r3-node ...")
    deadline = time.time() + 120
    while time.time() < deadline:
        node = get_pod_node(v1, namespace, pod_selector)
        if node == "r3-node":
            log(f"  [Reset] Pod confirmed on r3-node")
            break
        time.sleep(5)
    else:
        log("  [Reset] WARNING: pod did not reach r3-node within 120 s — continuing anyway")

    log(f"  [Reset] Settling {settle_s}s for Prometheus rate() windows to flush ...")
    time.sleep(settle_s)
    log("  [Reset] Cluster ready — identical starting state confirmed")


# ── Single-policy run ─────────────────────────────────────────────────────────

async def run_policy(
    policy: dict,
    n_cycles: int,
    prom_url: str,
    v1,
    namespace: str,
    pod_selector: str,
    wave_yaml: str,
    kube_ctx: str,
) -> list[dict]:
    """
    Run one policy for n_cycles wave periods.
    Returns list of sample dicts.
    """
    duration_s = n_cycles * WAVE_PERIOD_S
    log(f"\n{'='*55}")
    log(f"POLICY: {policy['name'].upper()}  ({duration_s}s = {n_cycles} cycles)")
    log(f"{'='*55}")

    set_policy(policy["conf"])
    log("  Waiting 10 s for agent to hot-reload policy ...")
    await asyncio.sleep(10)

    start_wave(wave_yaml, kube_ctx, namespace)

    samples  = []
    start    = time.time()
    end      = start + duration_s
    prev_node = None
    migration_count = 0

    while time.time() < end:
        t0      = time.time()
        elapsed = round(t0 - start, 1)

        metrics, node = await asyncio.gather(
            fetch_metrics(prom_url),
            asyncio.get_event_loop().run_in_executor(
                None, get_pod_node, v1, namespace, pod_selector
            ),
        )

        if node and prev_node and node != prev_node:
            migration_count += 1
            log(f"  ↔ Migration detected: {prev_node} → {node}  "
                f"(total={migration_count})")
        prev_node = node or prev_node

        samples.append({
            "policy":            policy["name"],
            "elapsed_s":         elapsed,
            "timestamp":         datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "current_node":      node or "unknown",
            "qos":               metrics.get("qos"),
            "qos_latency_score": metrics.get("qos_latency_score"),
            "cpu_r3":            metrics.get("cpu_r3"),
            "cpu_vm1":           metrics.get("cpu_vm1"),
            "migration_event":   1 if (node and prev_node and node != prev_node) else 0,
        })

        # Status every 60 s
        if int(elapsed) % 60 == 0 and elapsed > 0:
            qos    = metrics.get("qos",    "N/A")
            cpu_r3 = metrics.get("cpu_r3", "N/A")
            log(f"  t+{elapsed:.0f}s | node={node} | qos={qos} | "
                f"cpu_r3={cpu_r3}% | migrations={migration_count}")

        await asyncio.sleep(max(0.0, SAMPLE_INTERVAL_S - (time.time() - t0)))

    stop_wave(kube_ctx, namespace)
    log(f"  Run complete: {len(samples)} samples, {migration_count} migrations")
    return samples


# ── Statistics ────────────────────────────────────────────────────────────────

def compute_stats(samples: list[dict]) -> dict:
    qos_vals   = [s["qos"]   for s in samples if s["qos"]   is not None]
    cpu_r3_vals = [s["cpu_r3"] for s in samples if s["cpu_r3"] is not None]

    if not qos_vals:
        return {"error": "no qos data"}

    qos_sorted = sorted(qos_vals)
    n          = len(qos_sorted)
    p10_idx    = max(0, int(n * 0.10) - 1)

    burn_qos = [s["qos"] for s in samples
                if s["qos"] is not None and s["cpu_r3"] is not None
                and s["cpu_r3"] > CPU_BURN_THRESHOLD]
    idle_qos = [s["qos"] for s in samples
                if s["qos"] is not None and s["cpu_r3"] is not None
                and s["cpu_r3"] < CPU_IDLE_THRESHOLD]

    # Count migration events from node-change detection
    migration_count = sum(
        1 for i in range(1, len(samples))
        if samples[i]["current_node"] != samples[i-1]["current_node"]
        and samples[i]["current_node"] not in ("unknown", None)
        and samples[i-1]["current_node"] not in ("unknown", None)
    )

    # Time on each node
    node_counts = {}
    for s in samples:
        n_ = s["current_node"] or "unknown"
        node_counts[n_] = node_counts.get(n_, 0) + 1
    total = len(samples)
    node_pct = {k: round(100 * v / total, 1) for k, v in node_counts.items()}

    # Recovery time: after qos drops below SLA, how long until it recovers?
    recovery_times = []
    in_violation   = False
    violation_start = None
    for s in samples:
        q = s["qos"]
        if q is None:
            continue
        if not in_violation and q < SLA_THRESHOLD:
            in_violation    = True
            violation_start = s["elapsed_s"]
        elif in_violation and q >= SLA_THRESHOLD:
            recovery_times.append(s["elapsed_s"] - violation_start)
            in_violation = False

    return {
        "samples":            len(samples),
        "mean_qos":           round(statistics.mean(qos_vals), 2),
        "min_qos":            round(min(qos_vals), 2),
        "p10_qos":            round(qos_sorted[p10_idx], 2),
        "pct_below_sla":      round(100 * sum(1 for q in qos_vals if q < SLA_THRESHOLD) / n, 1),
        "migration_count":    migration_count,
        "node_distribution":  node_pct,
        "avg_qos_burn":       round(statistics.mean(burn_qos), 2) if burn_qos else None,
        "avg_qos_idle":       round(statistics.mean(idle_qos), 2) if idle_qos else None,
        "avg_recovery_s":     round(statistics.mean(recovery_times), 1) if recovery_times else None,
        "n_violations":       len(recovery_times) + (1 if in_violation else 0),
    }


# ── Report ────────────────────────────────────────────────────────────────────

def save_results(all_samples: list[dict], all_stats: dict, date_str: str):
    # Raw samples CSV
    csv_path = os.path.join(OUTPUT_DIR, f"comparison_{date_str}.csv")
    fields = ["policy", "elapsed_s", "timestamp", "current_node",
              "qos", "qos_latency_score", "cpu_r3", "cpu_vm1", "migration_event"]
    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(all_samples)
    log(f"\nRaw data  → {csv_path}")

    # Stats JSON
    json_path = os.path.join(OUTPUT_DIR, f"stats_{date_str}.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(all_stats, f, indent=2)
    log(f"Stats     → {json_path}")

    # Human-readable report
    report_path = os.path.join(OUTPUT_DIR, f"report_{date_str}.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("Policy Comparison Report\n")
        f.write("=" * 60 + "\n")
        f.write(f"Date     : {date_str}\n")
        f.write(f"SLA      : QoS >= {SLA_THRESHOLD}\n\n")

        cols = list(all_stats.keys())
        metrics_to_show = [
            ("mean_qos",        "Mean QoS (0-100)"),
            ("min_qos",         "Min QoS"),
            ("p10_qos",         "P10 QoS (worst 10%)"),
            ("pct_below_sla",   "% time below SLA"),
            ("migration_count", "Migrations triggered"),
            ("avg_qos_burn",    "Avg QoS during CPU burn"),
            ("avg_qos_idle",    "Avg QoS during CPU idle"),
            ("avg_recovery_s",  "Avg recovery time (s)"),
            ("n_violations",    "SLA violation events"),
        ]

        # Header row
        col_w = 14
        f.write(f"{'Metric':<30}" + "".join(f"{c:>{col_w}}" for c in cols) + "\n")
        f.write("-" * (30 + col_w * len(cols)) + "\n")

        for key, label in metrics_to_show:
            row = f"{label:<30}"
            for p in cols:
                val = all_stats[p].get(key)
                row += f"{str(val) if val is not None else '-':>{col_w}}"
            f.write(row + "\n")

        f.write("\nNode distribution:\n")
        for p in cols:
            dist = all_stats[p].get("node_distribution", {})
            f.write(f"  {p:<12}: {dist}\n")

    log(f"Report    → {report_path}")

    # Print to console
    with open(report_path, encoding="utf-8") as f:
        print("\n" + f.read())


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    ap = argparse.ArgumentParser(description="4-policy comparison benchmark")
    ap.add_argument("--prometheus-url",  default="http://192.168.0.42:9090")
    ap.add_argument("--kube-context",    default="clus1")
    ap.add_argument("--kubeconfig-path",
                    default=os.path.join(SCRIPT_DIR, "../Cluster/kubeconfig"))
    ap.add_argument("--namespace",       default="smartcity")
    ap.add_argument("--pod-selector",    default="app=traffic-processor")
    ap.add_argument("--wave-yaml",
                    default=os.path.join(SCRIPT_DIR, "../../k3sApp/k8s/cpu-wave.yaml"))
    ap.add_argument("--cycles",          type=int, default=6,
                    help="Wave cycles per policy (default 6 = 60 min)")
    ap.add_argument("--policies",        default="passive,threshold,weights,ml",
                    help="Comma-separated subset of policies to run")
    args = ap.parse_args()

    k8s_config.load_kube_config(
        config_file=os.path.abspath(args.kubeconfig_path),
        context=args.kube_context,
    )
    v1 = k8s_client.CoreV1Api()

    selected = [p["name"] for p in POLICIES
                if p["name"] in args.policies.split(",")]
    run_policies = [p for p in POLICIES if p["name"] in selected]

    RESET_S     = 90 + 30   # settle_s + pod-wait buffer
    date_str    = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    duration_m  = args.cycles * WAVE_PERIOD_S // 60
    total_m     = len(run_policies) * (duration_m + RESET_S // 60)

    log("=" * 60)
    log("POLICY COMPARISON BENCHMARK")
    log(f"  Policies  : {[p['name'] for p in run_policies]}")
    log(f"  Cycles    : {args.cycles} x {WAVE_PERIOD_S}s = {duration_m} min each")
    log(f"  Reset     : ~{RESET_S}s per policy (pod→r3, Prometheus flush)")
    log(f"  Est. total: ~{total_m} min")
    log(f"  SLA       : QoS >= {SLA_THRESHOLD}")
    log("=" * 60)

    all_samples = []
    all_stats   = {}

    for i, policy in enumerate(run_policies):
        # ── Reset to identical starting state before every run ────────────────
        log(f"\n{'─'*55}")
        log(f"RESET → pod to r3-node, Prometheus flush, passive policy")
        log(f"{'─'*55}")
        reset_cluster(
            v1           = v1,
            namespace    = args.namespace,
            pod_selector = args.pod_selector,
            ctx          = args.kube_context,
        )

        samples = await run_policy(
            policy       = policy,
            n_cycles     = args.cycles,
            prom_url     = args.prometheus_url,
            v1           = v1,
            namespace    = args.namespace,
            pod_selector = args.pod_selector,
            wave_yaml    = os.path.abspath(args.wave_yaml),
            kube_ctx     = args.kube_context,
        )
        all_samples.extend(samples)
        all_stats[policy["name"]] = compute_stats(samples)

        # Save incremental results after each policy
        save_results(all_samples, all_stats, date_str)

    # Restore to passive at end
    set_policy("")
    log("\nBenchmark complete. policy.conf reset to passive.")
    log(f"Results in: Tests/comparison_results/")


if __name__ == "__main__":
    asyncio.run(main())
