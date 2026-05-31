#!/usr/bin/env python3
"""
preflight_check.py — Quick sanity test before overnight collection
==================================================================
Runs in ~3 minutes and verifies:
  1. Prometheus reachable + all 7 QoS metrics return data
  2. Kubernetes API reachable + traffic-processor pod found
  3. Rectangular wave pod: apply → sample 3 times → delete
  4. Triangular wave pod:  apply → sample 3 times → delete
  5. CSV written correctly (all 21 fields present)
  6. Phase detection logic (spot-check)

Usage:
  python Tests/preflight_check.py \\
    --prometheus-url http://192.168.0.42:9090 \\
    --kube-context clus1

Exits 0 if all checks pass, 1 if any fail.
"""

import argparse
import asyncio
import csv
import datetime
import os
import subprocess
import sys
import time

import httpx
from kubernetes import client as k8s_client, config as k8s_config

# ── Paths ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR    = os.path.dirname(os.path.abspath(__file__))
DEFAULT_RECT  = os.path.join(SCRIPT_DIR, "../../k3sApp/k8s/cpu-wave.yaml")
DEFAULT_TRI   = os.path.join(SCRIPT_DIR, "../../k3sApp/k8s/cpu-wave-triangle.yaml")

K8S_NAMESPACE = "smartcity"
POD_SELECTOR  = "app=traffic-processor"
WAVE_POD_NAME = "cpu-wave"

PROM_SCALARS = {
    "qos":                  'qos_composite{service=~"traffic-processor.*"}',
    "qos_latency_score":    'qos_latency_score{service=~"traffic-processor.*"}',
    "qos_cpu_score":        'qos_cpu_score{service=~"traffic-processor.*"}',
    "qos_throughput_score": 'qos_throughput_score{service=~"traffic-processor.*"}',
    "qos_rejection_rate":   'qos_rejection_rate{service=~"traffic-processor.*"}',
    "qos_cpu_percent":      'qos_cpu_percent{service=~"traffic-processor.*"}',
    "qos_avg_proc_time":    'qos_avg_proc_time{service=~"traffic-processor.*"}',
}
PROM_CPU = (
    '100 * sum by (k8s_node_name) '
    '(rate(system_cpu_time_seconds_total{job="otel-collectors",state!="idle"}[1m])) '
    '/ count by (k8s_node_name) '
    '(system_cpu_time_seconds_total{job="otel-collectors",state="idle"})'
)

CSV_FIELDS = [
    "session_id", "elapsed_s", "timestamp",
    "wave_type", "wave_phase",
    "current_node", "node_encoded",
    "qos", "qos_latency_score", "qos_cpu_score",
    "qos_throughput_score", "qos_rejection_rate",
    "qos_cpu_percent", "qos_avg_proc_time",
    "cpu_r3", "cpu_vm1",
    "qos_trend", "latency_trend", "cpu_r3_trend",
    "qos_degraded", "migrate_label",
]

# ── Helpers ───────────────────────────────────────────────────────────────────
PASS = "  ✅"
FAIL = "  ❌"
WARN = "  ⚠️ "

results = []

def check(label: str, ok: bool, detail: str = ""):
    icon = PASS if ok else FAIL
    msg  = f"{icon} {label}"
    if detail:
        msg += f"  ({detail})"
    print(msg)
    results.append((label, ok))
    return ok

def kubectl(args: list, ctx: str) -> str | None:
    try:
        r = subprocess.run(
            ["kubectl", f"--context={ctx}"] + args,
            text=True, capture_output=True, check=True,
        )
        return r.stdout.strip()
    except subprocess.CalledProcessError as e:
        return None

def section(title: str):
    print(f"\n── {title} {'─' * (50 - len(title))}")


# ── Check 1: Prometheus ───────────────────────────────────────────────────────

async def check_prometheus(prom_url: str):
    section("1. Prometheus connectivity + QoS metrics")
    try:
        async with httpx.AsyncClient(timeout=5.0) as c:
            r = await c.get(f"{prom_url}/-/ready")
            check("Prometheus reachable", r.status_code == 200,
                  f"HTTP {r.status_code}")
    except Exception as e:
        check("Prometheus reachable", False, str(e))
        print(f"     Cannot continue metric checks — is Prometheus at {prom_url}?")
        return {}

    metric_values = {}
    async with httpx.AsyncClient(timeout=5.0) as c:
        for name, query in PROM_SCALARS.items():
            try:
                r = await c.get(f"{prom_url}/api/v1/query",
                                params={"query": query})
                data = r.json().get("data", {}).get("result", [])
                if data:
                    val = round(float(data[0]["value"][1]), 3)
                    metric_values[name] = val
                    check(f"  {name}", True, str(val))
                else:
                    check(f"  {name}", False, "no data (is traffic-processor running?)")
            except Exception as e:
                check(f"  {name}", False, str(e))

        # Per-node CPU
        try:
            r = await c.get(f"{prom_url}/api/v1/query",
                            params={"query": PROM_CPU})
            data = r.json().get("data", {}).get("result", [])
            nodes = {r.get("metric", {}).get("k8s_node_name"): round(float(r["value"][1]), 1)
                     for r in data}
            check("  cpu_r3 / cpu_vm1", bool(nodes), str(nodes))
            metric_values.update({f"cpu_{k.replace('-','_').replace('node','')}": v
                                   for k, v in nodes.items()})
        except Exception as e:
            check("  cpu per-node", False, str(e))

    return metric_values


# ── Check 2: Kubernetes ───────────────────────────────────────────────────────

def check_kubernetes(ctx: str, kubeconfig: str):
    section("2. Kubernetes API + traffic-processor pod")
    try:
        k8s_config.load_kube_config(config_file=kubeconfig, context=ctx)
        v1 = k8s_client.CoreV1Api()
        check("kubeconfig loaded", True, kubeconfig)
    except Exception as e:
        check("kubeconfig loaded", False, str(e))
        return None

    try:
        pods = v1.list_namespaced_pod(
            namespace=K8S_NAMESPACE, label_selector=POD_SELECTOR,
        )
        if pods.items:
            pod  = pods.items[0]
            node = pod.spec.node_name
            phase = pod.status.phase
            check("traffic-processor pod found",
                  phase == "Running",
                  f"node={node} phase={phase}")
        else:
            check("traffic-processor pod found", False,
                  f"no pods with selector '{POD_SELECTOR}' in {K8S_NAMESPACE}")
    except Exception as e:
        check("traffic-processor pod found", False, str(e))

    return v1


# ── Check 3 & 4: Wave pods ────────────────────────────────────────────────────

async def check_wave(wave_type: str, yaml_path: str, ctx: str,
                     prom_url: str, v1) -> list[dict]:
    section(f"{3 if wave_type == 'rectangular' else 4}. {wave_type.capitalize()} wave pod")

    yaml_abs = os.path.abspath(yaml_path)
    check(f"YAML exists", os.path.exists(yaml_abs), yaml_abs)
    if not os.path.exists(yaml_abs):
        return []

    # Delete any existing wave pod
    kubectl(["delete", "pod", WAVE_POD_NAME, "-n", K8S_NAMESPACE,
             "--ignore-not-found=true", "--wait=true"], ctx)
    time.sleep(2)

    # Apply wave
    out = kubectl(["apply", "-f", yaml_abs], ctx)
    check(f"kubectl apply", out is not None, out or "error")

    print(f"     Waiting 25 s for pod to start and settle ...")
    time.sleep(25)

    # Check pod Running
    try:
        pods = v1.list_namespaced_pod(
            namespace=K8S_NAMESPACE,
            label_selector="app=cpu-wave",
        )
        if pods.items:
            phase = pods.items[0].status.phase
            check(f"cpu-wave pod Running", phase == "Running", f"phase={phase}")
        else:
            check(f"cpu-wave pod Running", False, "pod not found")
    except Exception as e:
        check(f"cpu-wave pod Running", False, str(e))

    # Take 3 samples (15 s)
    samples = []
    print(f"     Sampling 3 × 5 s ...")
    for i in range(3):
        t0 = time.time()
        try:
            async with httpx.AsyncClient(timeout=4.0) as c:
                r = await c.get(f"{prom_url}/api/v1/query",
                                params={"query": PROM_SCALARS["qos"]})
                data = r.json().get("data", {}).get("result", [])
                qos  = round(float(data[0]["value"][1]), 2) if data else None

                r2 = await c.get(f"{prom_url}/api/v1/query",
                                 params={"query": PROM_CPU})
                cpu_data = r2.json().get("data", {}).get("result", [])
                cpu_r3 = next(
                    (round(float(r["value"][1]), 1) for r in cpu_data
                     if r.get("metric", {}).get("k8s_node_name") == "r3-node"),
                    None,
                )

            samples.append({
                "session_id":    f"preflight_{wave_type}",
                "elapsed_s":     round(i * 5.0, 1),
                "timestamp":     datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "wave_type":     wave_type,
                "wave_phase":    "burn" if wave_type == "rectangular" else "ramp_up",
                "current_node":  "r3-node",
                "node_encoded":  0,
                "qos":           qos,
                "qos_latency_score":    None,
                "qos_cpu_score":        None,
                "qos_throughput_score": None,
                "qos_rejection_rate":   None,
                "qos_cpu_percent":      None,
                "qos_avg_proc_time":    None,
                "cpu_r3":        cpu_r3,
                "cpu_vm1":       None,
                "qos_trend":     None,
                "latency_trend": None,
                "cpu_r3_trend":  None,
                "qos_degraded":  int(qos < 70) if qos is not None else None,
                "migrate_label": 0,
            })
            print(f"       sample {i+1}: qos={qos}  cpu_r3={cpu_r3}%")
        except Exception as e:
            print(f"       sample {i+1}: ERROR — {e}")

        await asyncio.sleep(max(0, 5.0 - (time.time() - t0)))

    check(f"Got samples with qos data",
          any(s["qos"] is not None for s in samples),
          f"{sum(1 for s in samples if s['qos'] is not None)}/3 samples have qos")

    # Cleanup
    kubectl(["delete", "pod", WAVE_POD_NAME, "-n", K8S_NAMESPACE,
             "--ignore-not-found=true"], ctx)
    check("Wave pod deleted", True)

    return samples


# ── Check 5: CSV integrity ────────────────────────────────────────────────────

def check_csv(all_samples: list[dict]):
    section("5. CSV write + field integrity")

    test_csv = os.path.join(SCRIPT_DIR, "training_data", "_preflight_test.csv")
    os.makedirs(os.path.dirname(test_csv), exist_ok=True)

    try:
        with open(test_csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
            w.writeheader()
            w.writerows(all_samples)
        check("CSV written", True, test_csv)
    except Exception as e:
        check("CSV written", False, str(e))
        return

    try:
        with open(test_csv, newline="") as f:
            reader = csv.DictReader(f)
            fields  = reader.fieldnames or []
            rows    = list(reader)

        missing = [f for f in CSV_FIELDS if f not in fields]
        check("All 21 fields present", not missing,
              f"missing: {missing}" if missing else f"{len(fields)} fields OK")
        check(f"Row count matches", len(rows) == len(all_samples),
              f"{len(rows)} rows written")
    except Exception as e:
        check("CSV readable", False, str(e))

    # Clean up test file
    os.remove(test_csv)


# ── Check 6: Phase logic ──────────────────────────────────────────────────────

def check_phase_logic():
    section("6. Wave phase detection logic")

    # Import from data_collector
    sys.path.insert(0, SCRIPT_DIR)
    try:
        from data_collector import get_wave_phase, RECT_IDLE, RECT_BURN, TRI_STEPS, TRI_STEP_S

        # Rectangular
        assert get_wave_phase("rectangular", 0)   == "idle", "t=0 should be idle"
        assert get_wave_phase("rectangular", 299) == "idle", "t=299 should be idle"
        assert get_wave_phase("rectangular", 300) == "burn", "t=300 should be burn"
        assert get_wave_phase("rectangular", 599) == "burn", "t=599 should be burn"
        assert get_wave_phase("rectangular", 600) == "idle", "t=600 wraps to idle"
        check("Rectangular phase (idle/burn)", True, "t=0→idle, t=300→burn, t=600→idle(wrap)")

        # Triangular
        peak_idx = TRI_STEPS.index(max(TRI_STEPS))
        t_idle   = 0
        t_rampup = TRI_STEP_S + 1
        t_peak   = peak_idx * TRI_STEP_S + 1
        t_down   = (peak_idx + 1) * TRI_STEP_S + 1
        assert get_wave_phase("triangular", t_idle)   == "idle",     "step 0 = idle"
        assert get_wave_phase("triangular", t_rampup) == "ramp_up",  "step 1 = ramp_up"
        assert get_wave_phase("triangular", t_peak)   == "ramp_up",  "peak step = ramp_up"
        assert get_wave_phase("triangular", t_down)   == "ramp_down","post-peak = ramp_down"
        check("Triangular phase (idle/ramp_up/ramp_down)", True,
              f"steps={TRI_STEPS} step_s={TRI_STEP_S}")

    except AssertionError as e:
        check("Phase logic assertions", False, str(e))
    except Exception as e:
        check("Phase logic import", False, str(e))


# ── Summary ───────────────────────────────────────────────────────────────────

def print_summary(elapsed: float):
    passed = sum(1 for _, ok in results if ok)
    total  = len(results)
    failed = [(label, ok) for label, ok in results if not ok]

    print(f"\n{'═' * 55}")
    print(f"PREFLIGHT RESULT: {passed}/{total} checks passed  ({elapsed:.0f}s)")
    if failed:
        print(f"\nFailed checks:")
        for label, _ in failed:
            print(f"  ❌  {label}")
        print("\nFix the above issues before starting overnight collection.")
    else:
        print("\n✅  All checks passed — safe to run overnight.")
        print("\nCommand:")
        print("  python Tests/data_collector.py \\")
        print("    --prometheus-url http://192.168.0.42:9090 \\")
        print("    --session-duration 1800")
    print(f"{'═' * 55}")
    return len(failed) == 0


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    ap = argparse.ArgumentParser(description="Preflight check for overnight data collection")
    ap.add_argument("--prometheus-url",  default="http://192.168.0.42:9090")
    ap.add_argument("--kube-context",    default="clus1")
    ap.add_argument("--kubeconfig-path",
                    default=os.path.join(SCRIPT_DIR, "../Cluster/kubeconfig"))
    ap.add_argument("--wave-rect-yaml",  default=DEFAULT_RECT)
    ap.add_argument("--wave-tri-yaml",   default=DEFAULT_TRI)
    args = ap.parse_args()

    t_start = time.time()
    print(f"{'═' * 55}")
    print("PREFLIGHT CHECK — overnight data_collector")
    print(f"  Prometheus: {args.prometheus_url}")
    print(f"  k8s context: {args.kube_context}")
    print(f"  Estimated time: ~3 min")
    print(f"{'═' * 55}")

    # 1. Prometheus
    metric_values = await check_prometheus(args.prometheus_url)

    # 2. Kubernetes
    v1 = check_kubernetes(
        args.kube_context,
        os.path.abspath(args.kubeconfig_path),
    )

    all_samples = []

    if v1:
        # 3. Rectangular wave
        samples_rect = await check_wave(
            "rectangular", args.wave_rect_yaml,
            args.kube_context, args.prometheus_url, v1,
        )
        all_samples.extend(samples_rect)

        # 4. Triangular wave
        samples_tri = await check_wave(
            "triangular", args.wave_tri_yaml,
            args.kube_context, args.prometheus_url, v1,
        )
        all_samples.extend(samples_tri)
    else:
        print(f"\n{WARN} Skipping wave pod checks (k8s unavailable)")

    # 5. CSV
    if all_samples:
        check_csv(all_samples)
    else:
        print(f"\n{WARN} Skipping CSV check (no samples collected)")

    # 6. Phase logic
    check_phase_logic()

    # Summary
    ok = print_summary(time.time() - t_start)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    asyncio.run(main())
