# CPAM: Eliminating the Security–QoS Trade-off in SDN-based Moving Target Defense

This repository contains the full implementation, benchmark scripts, and experimental results for the paper:

> **"CPAM: Eliminating the Security–QoS Trade-off in SDN-based Moving Target Defense"**

---

## Overview

CPAM is a Moving Target Defense (MTD) SDN controller built on top of the Ryu framework. It continuously rotates Virtual IPs (VIPs) assigned to hosts every 60 seconds, making it harder for adversaries to map the network through reconnaissance. The key contribution is that CPAM maintains network connectivity and QoS while keeping MTD active — evaluated against a standard baseline Ryu controller on the same topology.

---

## Repository Structure

```
.
├── Controllers
│   ├── mtd.py                   # CPAM controller (MTD, VIP rotation)
│   └── baseline.py               # Baseline Ryu controller (no MTD, BFS routing)
│
├── Topology
│   ├── industry_topo.py          # Large topology (~504 hosts, 48-port switches)
│   └── simpletp.py               # Simple test topology
│
├── Benchmark Scripts
│   ├── benchmark_test.sh         # Full benchmark for CPAM (all metrics)
│   └── benchmark_baseline.sh     # Full benchmark for baseline (all metrics)
│
│
├── Results
│   ├── baseline_10 runs/         # 10 baseline benchmark iterations
│   │   └── baseline_nsdi_baseline_large_iter{1-10}_*/
│   ├── MTD_10 runs/              # 10 CPAM benchmark iterations
│   │   └── cpam_nsdi_baseline_large_iter{1-10}_*/
│   ├── tcp_10_runs/              # 10 dedicated TCP benchmark runs (CPAM, long mode)
│   │   └── tcp_bench_baseline_large_iter{1-10}/
│   └── plots/                    # Generated comparison plots (PNG + PDF)

```

---

## Testbed

| Component | Details |
|-----------|---------|
| Emulator | Mininet |
| Topology | Large — 1 core + 1 aggregation + 21 edge switches (48-port), 504 hosts |
| Switch pairs | 252 host pairs (first half → second half) |
| SDN Framework | Ryu OpenFlow 1.3 |
| OS | Ubuntu 20.04 |
| Benchmark duration | 120s per QoS test, 240s long TCP, 60s churn |
| Iterations | 10 runs per system |

---

## Controllers

### CPAM (`mtd.py`)
- Assigns each host a Virtual IP (VIP) from a pool of 6,000 addresses
- Rotates all VIPs every **60 seconds** (configurable via `ROTATE_INTERVAL`)
- Implements NAT translation: client connects to VIP → controller rewrites to real IP
- GRACE state mechanism keeps old VIPs alive until active sessions end
- VIP state written to `/tmp/mtd_vip_mapping.json` for benchmark consumption

### Baseline (`baseline.py`)
- Standard L2 learning switch with BFS shortest-path routing
- Installs flows along the **full path** across all switches
- No address translation — uses real IPs directly

---

## Running the Experiments

### Prerequisites

```bash
# Install dependencies
sudo apt-get install -y mininet openvswitch-switch python3-pip sysstat iperf
pip3 install ryu

# Start the CPAM controller
ryu-manager mtd.py

# Or start the baseline controller
ryu-manager baseline.py
```

### Run baseline benchmark (10 iterations)

```bash
sudo bash benchmark_baseline.sh 120 baseline_large /var/log/ryu.log large 10
```

### Run CPAM benchmark (10 iterations)

```bash
sudo bash benchmark_nsdi.sh 120 baseline_large /var/log/ryu.log large 10
```

### Generate comparison plots

```bash
pip3 install matplotlib

python3 compare_metrics.py \
    --baseline-dir "baseline_10 runs" \
    --mtd-dir      "MTD_10 runs" \
    --output-dir   plots

python3 plot_baseline_runs.py --dir "baseline_10 runs" --out baseline_plots
python3 plot_mtd_runs.py      --dir "MTD_10 runs"      --out mtd_plots
```

---

## Metrics Measured

| ID | Metric | Description |
|----|--------|-------------|
| A | ICMP Latency | Mean RTT across 252 pairs (ping, 120s) |
| B | TCP Throughput | Mean bandwidth across 252 pairs (iperf, full line rate) |
| C | TCP Session Continuity | Long-lived session survival across VIP rotations |
| D+E | UDP Jitter + Loss | Per-pair jitter and loss (iperf -u, 100K/pair, 120s) |
| F | Controller Overhead | CPU % and RSS memory (pidstat) |
| 1 | Topology Discovery | Time for controller to discover all switches and links |
| 2 | Async Message Rate | PacketIn processing rate under connection churn |
| 3+N2 | RPPT | Reactive Path-Programming Time (idle + under load) |
| 4 | Forwarding Table | Peak OVS flow entry count |
| 5 | NRT | Network Repair Time after simulated link failure |
| N1 | Flow Setup Rate | Flows installed per second during churn |
| N4 | VIP Reclamation Lag | Time from session end to VIP returned to pool (CPAM only) |

---


## Known Limitations and Future Work

1. **TCP throughput under VIP translation** — The current implementation installs VIP NAT flows only on the ingress switch and falls back to `OFPP_FLOOD` when the destination MAC is not locally known. In a multi-switch topology, this causes every TCP packet to be flooded across all switch ports, severely limiting throughput. The fix — installing flows along the full BFS path (identical to the baseline approach) — has been designed and is a priority for the next release.

2. **Simultaneous VIP rotation** — All 504 hosts currently rotate at the same instant, creating a short PacketIn burst. A staggered rotation implementation (20 hosts per batch, 150 ms between batches) is already in the codebase but not yet evaluated.

3. **RPPT instrumentation missing from CPAM** — The baseline logs `RPPT_MEASURED` tokens for every flow setup, enabling latency percentile analysis. CPAM does not yet emit this token.

4. **Controller memory growth** — CPAM RSS grows from ~71 MB to ~83 MB across 10 consecutive runs when the controller process is not restarted, suggesting accumulated VIP state. A periodic cleanup routine is planned.

---

## Results File Format

Each benchmark iteration folder contains:

| File | Contents |
|------|----------|
| `A_latency_summary.txt` | ICMP RTT per pair + topology mean |
| `DE_udp_summary.txt` | UDP jitter, loss, session drop counts |
| `B_tcp_throughput_summary.txt` | TCP bandwidth statistics (baseline only) |
| `C_session_continuity_summary.txt` | Long-lived session survival rate |
| `F_controller_overhead_summary.txt` | CPU and RSS from pidstat |
| `F_controller_pidstat.txt` | Raw pidstat output (1-second samples) |
| `2_async_rate_summary.txt` | Async message processing rate |
| `3_rppt_summary.txt` | RPPT latency statistics |
| `4_forwarding_table_capacity.txt` | OVS flow table snapshots |
| `5_nrt_timing.txt` | Link failure/restore timestamps and NRT |
| `N1_flow_setup_rate.txt` | Flow setup rate during churn |
| `SUMMARY.txt` (CPAM) / `FINAL_all_metrics_summary.txt` (baseline) | Master summary |

---

## Dependencies

| Tool | Version | Purpose |
|------|---------|---------|
| Python | 3.8+ | Controllers and analysis scripts |
| Ryu | 4.34+ | SDN controller framework |
| Mininet | 2.3+ | Network emulation |
| Open vSwitch | 2.13+ | Virtual switch |
| iperf | 2.x | TCP/UDP traffic generation |
| sysstat | any | `pidstat` for controller profiling |
| matplotlib | 3.x | Plot generation |

---

## Authors

- Souhail — sc3130@msstate.edu
- Umesh Biswas — ucb5@msstate.edu
- Charan Gudla — gudla@cse.msstate.edu

---

## License

This project is released for academic research purposes.
