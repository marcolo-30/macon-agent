#!/usr/bin/env python3
"""
data_collector.py  v3 — overnight QoS regression data collection
=================================================================
Collects training data for a QoS regressor (Option A).

The model will learn:
    predicted_qos_r3 = f(cpu_r3, cpu_vm1, trends, ...)

At inference the agent uses predicted_qos_r3 to decide:
    on r3  : migrate to vm1  if predicted_qos_r3 < MIGRATE_THRESHOLD (70)
    on vm1 : return  to r3   if predicted_qos_r3 > RETURN_THRESHOLD  (80)

Only rectangular wave, pod always on r3.
qos is the regression TARGET — no binary labels needed.

CSV fields (18)
───────────────
  session_id, elapsed_s, timestamp
  current_node, node_encoded
  qos  ← REGRESSION TARGET
  qos_latency_score, qos_cpu_score, qos_throughput_score,
  qos_rejection_rate, qos_cpu_percent, qos_avg_proc_time
  cpu_r3, cpu_vm1
  qos_trend, latency_trend, cpu_r3_trend, cpu_vm1_trend

Usage
─────
  python Tests/data_collector.py \\
    --prometheus-url http://192.168.0.42:9090

Stop: Ctrl+C  → finishes current session, saves, exits cleanly.

Requirements: pip install httpx kubernetes
"""

import argparse
import asyncio
import collections
import csv
import datetime
import os
import signal
import subprocess
import time

import httpx
from kubernetes import client as k8s_client, config as k8s_config

# ── Paths ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR        = os.path.dirname(os.path.abspath(__file__))
TRAINING_DATA_DIR = os.path.join(SCRIPT_DIR, "training_data")
os.makedirs(TRAINING_DATA_DIR, exist_ok=True)

# ── Kubernetes ────────────────────────────────────────────────────────────────
K8S_NAMESPACE = "smartcity"
POD_SELECTOR  = "app=traffic-processor"
WAVE_POD_NAME = "cpu-wave"

# ── Sampling ──────────────────────────────────────────────────────────────────
SAMPLE_INTERVAL_S = 5
TREND_SAMPLES     = 6       # 6 × 5 s = 30 s rolling window

# ── Prometheus queries ────────────────────────────────────────────────────────
PROM_SCALARS = {
    "qos":                  'qos_composite{service=~"traffic-processor.*"}',
    "qos_latency_score":    'qos_latency_score{service=~"traffic-processor.*"}',
    "qos_cpu_score":        'qos_cpu_score{service=~"traffic-processor.*"}',
    "qos_throughput_score": 'qos_throughput_score{service=~"traffic-processor.*"}',
    "qos_rejection_rate":   'qos_rejection_rate{service=~"traffic-processor.*"}',
    "qos_cpu_percent":      'qos_cpu_percent{service=~"traffic-processor.*"}',
    "qos_avg_proc_time":    'qos_avg_proc_time{service=~"traffic-processor.*"}',
}
PROM_CPU_PER_NODE = (
    '100 * sum by (k8s_node_name) '
    '(rate(system_cpu_time_seconds_total{job="otel-collectors",state!="idle"}[1m])) '
    '/ count by (k8s_node_name) '
    '(system_cpu_time_seconds_total{job="otel-collectors",state="idle"})'
)

CSV_FIELDS = [
    "session_id", "elapsed_s", "timestamp",
    "current_node", "node_encoded",
    # ── regression target ──────────────────────────────────────
    "qos",
    # ── app QoS sub-scores ─────────────────────────────────────
    "qos_latency_score", "qos_cpu_score",
    "qos_throughput_score", "qos_rejection_rate",
    "qos_cpu_percent", "qos_avg_proc_time",
    # ── system metrics (always available, both nodes) ──────────
    "cpu_r3", "cpu_vm1",
    # ── rolling trends (30 s window) ──────────────────────────
    "qos_trend", "latency_trend", "cpu_r3_trend", "cpu_vm1_trend",
]

# ── Global stop flag ──────────────────────────────────────────────────────────
_stop = False

def _on_signal(sig, _):
    global _stop
    print(f"\n[{_ts()}] Signal {sig} — finishing current session then saving ...",
          flush=True)
    _stop = True

signal.signal(signal.SIGINT,  _on_signal)
signal.signal(signal.SIGTERM, _on_signal)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ts() -> str:
    return datetime.datetime.now().strftime("%H:%M:%S")

def log(msg: str):
    print(f"[{_ts()}] {msg}", flush=True)

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

def get_pod_node(v1) -> str | None:
    try:
        pods = v1.list_namespaced_pod(
            namespace=K8S_NAMESPACE, label_selector=POD_SELECTOR,
        )
        return pods.items[0].spec.node_name if pods.items else None
    except Exception:
        return None


# ── Prometheus fetch ──────────────────────────────────────────────────────────

async def fetch_metrics(prom_url: str) -> dict:
    """Fetch all QoS scalars + per-node CPU in one parallel batch."""
    queries = {**PROM_SCALARS, "_cpu": PROM_CPU_PER_NODE}
    names   = list(queries.keys())

    async with httpx.AsyncClient(timeout=4.0) as c:
        responses = await asyncio.gather(
            *[c.get(f"{prom_url}/api/v1/query", params={"query": q})
              for q in queries.values()],
            return_exceptions=True,
        )

    result: dict = {}
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


# ── Session collection ────────────────────────────────────────────────────────

async def collect_session(
    session_id: str,
    duration:   int,
    prom_url:   str,
    v1,
) -> list[dict]:
    """
    Collect one session of raw samples.
    qos is the regression target — no labels computed here.
    Returns list of sample dicts.
    """
    samples: list[dict] = []

    # Rolling buffers for trend computation
    trend_buf = {
        "qos":             collections.deque(maxlen=TREND_SAMPLES + 1),
        "qos_latency_score": collections.deque(maxlen=TREND_SAMPLES + 1),
        "cpu_r3":          collections.deque(maxlen=TREND_SAMPLES + 1),
        "cpu_vm1":         collections.deque(maxlen=TREND_SAMPLES + 1),
    }

    start    = time.time()
    end      = start + duration
    status_t = start

    log(f"Collecting | session={session_id} | duration={duration}s")

    while time.time() < end and not _stop:
        t0      = time.time()
        elapsed = round(t0 - start, 1)

        metrics, node = await asyncio.gather(
            fetch_metrics(prom_url),
            asyncio.get_event_loop().run_in_executor(None, get_pod_node, v1),
        )

        # Update trend buffers
        for key in trend_buf:
            v = metrics.get(key)
            if v is not None:
                trend_buf[key].append(v)

        def trend(key: str):
            b = trend_buf[key]
            if len(b) < TREND_SAMPLES + 1:
                return None
            return round((b[-1] - b[0]) / TREND_SAMPLES, 4)

        samples.append({
            "session_id":           session_id,
            "elapsed_s":            elapsed,
            "timestamp":            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "current_node":         node or "unknown",
            "node_encoded":         1 if node == "vm1node" else 0,
            "qos":                  metrics.get("qos"),
            "qos_latency_score":    metrics.get("qos_latency_score"),
            "qos_cpu_score":        metrics.get("qos_cpu_score"),
            "qos_throughput_score": metrics.get("qos_throughput_score"),
            "qos_rejection_rate":   metrics.get("qos_rejection_rate"),
            "qos_cpu_percent":      metrics.get("qos_cpu_percent"),
            "qos_avg_proc_time":    metrics.get("qos_avg_proc_time"),
            "cpu_r3":               metrics.get("cpu_r3"),
            "cpu_vm1":              metrics.get("cpu_vm1"),
            "qos_trend":            trend("qos"),
            "latency_trend":        trend("qos_latency_score"),
            "cpu_r3_trend":         trend("cpu_r3"),
            "cpu_vm1_trend":        trend("cpu_vm1"),
        })

        # Status every 5 minutes
        if t0 - status_t >= 300:
            qos    = metrics.get("qos",    "N/A")
            cpu_r3 = metrics.get("cpu_r3", "N/A")
            log(f"  STATUS t+{elapsed}s | qos={qos} | cpu_r3={cpu_r3}% | "
                f"samples={len(samples)}")
            status_t = t0

        await asyncio.sleep(max(0.0, SAMPLE_INTERVAL_S - (time.time() - t0)))

    log(f"Session done: {len(samples)} raw samples")
    return samples


# ── Filter ────────────────────────────────────────────────────────────────────

def filter_samples(samples: list[dict]) -> list[dict]:
    """Drop samples where qos or cpu_r3 is missing."""
    before = len(samples)
    result = [s for s in samples if s.get("qos") is not None
                                 and s.get("cpu_r3") is not None]
    dropped = before - len(result)
    if dropped:
        log(f"  Dropped {dropped} incomplete samples")
    return result


# ── CSV ───────────────────────────────────────────────────────────────────────

def append_csv(rows: list[dict], path: str):
    write_header = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        if write_header:
            w.writeheader()
        w.writerows(rows)
    log(f"  Saved +{len(rows)} rows → {os.path.basename(path)}")


# ── Wave pod management ───────────────────────────────────────────────────────

def start_wave(yaml_path: str, ctx: str) -> None:
    kubectl(["delete", "pod", WAVE_POD_NAME, "-n", K8S_NAMESPACE,
             "--ignore-not-found=true", "--wait=true"], ctx)
    time.sleep(3)
    kubectl(["apply", "-f", yaml_path], ctx)
    log("Wave pod applied — waiting 20 s to stabilise ...")
    time.sleep(20)

def stop_wave(ctx: str):
    kubectl(["delete", "pod", WAVE_POD_NAME, "-n", K8S_NAMESPACE,
             "--ignore-not-found=true"], ctx)


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    ap = argparse.ArgumentParser(
        description="Overnight QoS regression data collector v3"
    )
    ap.add_argument("--prometheus-url",   default="http://192.168.0.42:9090")
    ap.add_argument("--kube-context",     default="clus1")
    ap.add_argument("--kubeconfig-path",
                    default=os.path.join(SCRIPT_DIR, "../Cluster/kubeconfig"))
    ap.add_argument("--session-duration", type=int, default=1800,
                    help="Seconds per session (default 1800 = 30 min)")
    ap.add_argument("--cooldown",         type=int, default=60,
                    help="Seconds between sessions (default 60)")
    ap.add_argument("--wave-yaml",
                    default=os.path.join(SCRIPT_DIR,
                                         "../../k3sApp/k8s/cpu-wave.yaml"))
    args = ap.parse_args()

    k8s_config.load_kube_config(
        config_file=os.path.abspath(args.kubeconfig_path),
        context=args.kube_context,
    )
    v1 = k8s_client.CoreV1Api()

    date_str   = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    master_csv = os.path.join(TRAINING_DATA_DIR, f"overnight_{date_str}.csv")
    wave_yaml  = os.path.abspath(args.wave_yaml)

    log("=" * 60)
    log("OVERNIGHT QoS REGRESSION DATA COLLECTOR  v3")
    log(f"  Output   : {master_csv}")
    log(f"  Session  : {args.session_duration} s | Cooldown: {args.cooldown} s")
    log(f"  Wave     : rectangular only ({wave_yaml})")
    log(f"  Target   : qos  (regression)")
    log(f"  Stop     : Ctrl+C  (saves cleanly)")
    log("=" * 60)

    session_num  = 0
    total_rows   = 0
    t_global     = time.time()

    while not _stop:
        session_num += 1
        session_id  = f"s{session_num}_{datetime.datetime.now().strftime('%H%M')}"

        log(f"\n{'─' * 55}")
        log(f"SESSION {session_num} | id={session_id}")
        log(f"{'─' * 55}")

        start_wave(wave_yaml, args.kube_context)

        raw  = await collect_session(
            session_id = session_id,
            duration   = args.session_duration,
            prom_url   = args.prometheus_url,
            v1         = v1,
        )
        rows = filter_samples(raw)
        total_rows += len(rows)

        qos_vals = [r["qos"] for r in rows if r["qos"] is not None]
        if qos_vals:
            log(f"  qos  min={min(qos_vals):.1f}  max={max(qos_vals):.1f}  "
                f"mean={sum(qos_vals)/len(qos_vals):.1f}")

        append_csv(rows, master_csv)
        stop_wave(args.kube_context)

        if not _stop:
            log(f"Cooldown {args.cooldown} s ...")
            for _ in range(args.cooldown):
                if _stop:
                    break
                await asyncio.sleep(1)

    elapsed_h = (time.time() - t_global) / 3600
    log(f"\n{'=' * 60}")
    log("COLLECTION COMPLETE")
    log(f"  Sessions : {session_num}")
    log(f"  Rows     : {total_rows}")
    log(f"  Duration : {elapsed_h:.2f} h")
    log(f"  CSV      : {master_csv}")
    log(f"{'=' * 60}")
    log(f"Next: python Tests/train_model.py --data {master_csv}")


if __name__ == "__main__":
    asyncio.run(main())
