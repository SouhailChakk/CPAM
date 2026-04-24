#!/usr/bin/env bash
# =============================================================================
#  Baseline Ryu NSDI Benchmark Script
#
#  Same topology assumptions and nearly the same workflow as benchmark_test.sh,
#  but targets a normal baseline controller with stable real IPs instead of VIPs.
#
#  Metrics:
#    [1]   Topology discovery time      — parsed from TOPO_DISCOVERY_COMPLETE
#    [2]   Async/Churn rate             — total churn connections / elapsed time
#    [3]   RPPT (idle)                  — parsed from RPPT_MEASURED
#    [4]   Forwarding table snapshots   — OVS flow counts
#    [5]   NRT                          — simple link-failure / ping recovery timing
#    [A]   ICMP latency                 — ping RTT/loss across fixed pairs
#    [B]   TCP throughput               — concurrent iperf2 TCP
#    [C]   TCP session continuity       — one long-lived TCP flow
#    [D+E] UDP jitter + loss            — iperf2 -u server summaries
#    [F]   Controller CPU + memory      — pidstat on ryu-manager
#    [N1]  Flow setup rate              — FLOW_SETUP count / churn elapsed
#    [N2]  RPPT percentiles             — p50/p95/p99 from idle RPPT events
#    [N3]  RPPT under load              — p50/p95/p99 during churn
#    [N4]  VIP reclamation lag          — N/A for baseline
#    [N5]  Scalability snapshot         — structured summary
# =============================================================================

set -uo pipefail
trap 'echo "[ERROR] Script failed at line $LINENO — command: $BASH_COMMAND" >&2' ERR

DUR="${1:-120}"
LABEL="${2:-baseline}"
RYU_LOG="${3:-/var/log/ryu.log}"
TOPOLOGY="${4:-small}"
ITERATIONS="${5:-1}"

if [[ "$ITERATIONS" -gt 1 ]]; then
    for _iter in $(seq 1 "$ITERATIONS"); do
        echo ""
        echo "╔══════════════════════════════════════════════════════════════════╗"
        echo "║  ITERATION ${_iter} / ${ITERATIONS}"
        echo "╚══════════════════════════════════════════════════════════════════╝"
        bash "$0" "$DUR" "${LABEL}_iter${_iter}" "$RYU_LOG" "$TOPOLOGY"
    done
    echo ""
    echo "[DONE] Completed $ITERATIONS iterations — results in: baseline_nsdi_${LABEL}_iter*"
    exit 0
fi

LONG_DUR="$((DUR * 2))"
TS="$(date +%Y%m%d_%H%M%S)"
OUTDIR="baseline_nsdi_${LABEL}_${TS}"
mkdir -p "$OUTDIR"

case "$TOPOLOGY" in
    small)
        TOPO_LABEL="Small (~16 hosts, 4-port switches)"
        FLOOD_BW_STEPS=("1M" "5M" "10M" "20M")
        CHURN_CONNS=60
        CHURN_CONN_DUR=1
        UDP_RATE_PER_PAIR="100K"
        UDP_MAX_PAIRS=0
        UDP_START_STAGGER="0.03"
        ;;
    medium)
        TOPO_LABEL="Medium (~100 hosts, 24-port switches)"
        FLOOD_BW_STEPS=("1M" "5M" "10M")
        CHURN_CONNS=40
        CHURN_CONN_DUR=1
        UDP_RATE_PER_PAIR="100K"
        UDP_MAX_PAIRS=0
        UDP_START_STAGGER="0.02"
        ;;
    large)
        TOPO_LABEL="Large (~500+ hosts, 48-port switches)"
        FLOOD_BW_STEPS=("1M" "4M")
        CHURN_CONNS=20
        CHURN_CONN_DUR=1
        UDP_RATE_PER_PAIR="100K"
        UDP_MAX_PAIRS=0
        UDP_START_STAGGER="0.01"
        ;;
    *)
        echo "[ERROR] Unknown topology: '$TOPOLOGY'. Use: small | medium | large"
        exit 1
        ;;
esac

CHURN_DUR=$(( CHURN_CONNS * CHURN_CONN_DUR ))
[[ "$CHURN_DUR" -gt "$DUR" ]] && CHURN_DUR="$DUR"

cat << BANNER
╔══════════════════════════════════════════════════════════════════╗
║         Baseline Ryu NSDI Benchmark                              ║
╠══════════════════════════════════════════════════════════════════╣
║  Label      : ${LABEL}
║  Topology   : ${TOPO_LABEL}
║  Duration   : ${DUR}s per QoS test  (long TCP: ${LONG_DUR}s)
║  Churn      : ${CHURN_DUR}s  (${CHURN_CONNS} conns/pair x ${CHURN_CONN_DUR}s each)
║  Flood steps: ${FLOOD_BW_STEPS[*]}
║  UDP rate   : ${UDP_RATE_PER_PAIR}/pair
║  UDP max    : ${UDP_MAX_PAIRS} pair cap (0 = all)
║  Ryu log    : ${RYU_LOG}
║  Output     : ${OUTDIR}
╠══════════════════════════════════════════════════════════════════╣
║  MODEL 1 (fixed pairs, stable IPs)   -> QoS metrics A B C D E
║  MODEL 2 (connection churn)          -> controller metrics 2 N1 N3
╚══════════════════════════════════════════════════════════════════╝
BANNER

MISSING=0
for bin in mnexec iperf ping awk sed grep pgrep ovs-vsctl ovs-ofctl date bc; do
    command -v "$bin" >/dev/null 2>&1 || { echo "[ERROR] Missing: $bin"; MISSING=1; }
done
[[ "$MISSING" -eq 1 ]] && exit 1

HAS_PIDSTAT=1
command -v pidstat >/dev/null 2>&1 || {
    HAS_PIDSTAT=0
    echo "[WARN] pidstat not found — install: sudo apt-get install -y sysstat"
}

if [[ ! -f "$RYU_LOG" ]]; then
    echo "[WARN] Ryu log not found at $RYU_LOG"
    echo "[WARN] Discovery/RPPT/FSR metrics require the controller log"
    RYU_LOG=""
fi

echo ""
echo "[INIT] Discovering Mininet hosts..."
declare -A HOST_PIDS
declare -A HOST_IPS
for i in $(seq 1 1000); do
    pid="$(pgrep -n -f " mininet:h${i}$" 2>/dev/null || true)"
    [[ -z "$pid" ]] && break
    HOST_PIDS["h${i}"]="$pid"
    ip="$(mnexec -a "$pid" ip -4 addr show 2>/dev/null | awk '/inet /{print $2}' | grep '^10\.' | head -1 | cut -d/ -f1 || true)"
    HOST_IPS["h${i}"]="${ip:-unknown}"
done
FOUND="${#HOST_PIDS[@]}"
echo "[INIT] Found $FOUND hosts"
[[ "$FOUND" -lt 2 ]] && { echo "[ERROR] Need at least 2 hosts. Is Mininet running?"; exit 1; }

RYUPID="$(pgrep -n -f 'ryu-manager' 2>/dev/null || true)"
[[ -z "$RYUPID" ]] && echo "[WARN] ryu-manager not found — controller metrics skipped"
[[ -n "$RYUPID" ]] && echo "[INIT] Ryu PID=$RYUPID"

declare -a PAIRS_SRC=()
declare -a PAIRS_DST=()
HOSTS_SORTED=($(echo "${!HOST_PIDS[@]}" | tr ' ' '\n' | sort -V))
HOST_COUNT="${#HOSTS_SORTED[@]}"
HALF=$(( HOST_COUNT / 2 ))

for i in $(seq 0 $((HALF - 1))); do
    src="${HOSTS_SORTED[$i]}"
    dst="${HOSTS_SORTED[$((i + HALF))]}"
    [[ -n "${HOST_PIDS[$src]:-}" && -n "${HOST_PIDS[$dst]:-}" ]] || continue
    PAIRS_SRC+=("$src")
    PAIRS_DST+=("$dst")
done

ODD_EXTRA_PAIR=0
if (( HOST_COUNT % 2 == 1 )); then
    extra_src="${HOSTS_SORTED[$((HOST_COUNT - 1))]}"
    extra_dst="${HOSTS_SORTED[0]}"
    if [[ "$extra_src" != "$extra_dst" && -n "${HOST_PIDS[$extra_src]:-}" && -n "${HOST_PIDS[$extra_dst]:-}" ]]; then
        PAIRS_SRC+=("$extra_src")
        PAIRS_DST+=("$extra_dst")
        ODD_EXTRA_PAIR=1
    fi
fi
NUM_PAIRS="${#PAIRS_SRC[@]}"

echo "[INIT] Fixed pairs: $NUM_PAIRS"
echo "[INIT] Hosts used : $HOST_COUNT discovered / $HOST_COUNT participating"
echo "[INIT] Pairing    : first half -> second half${ODD_EXTRA_PAIR:+, plus extra last-host pair}"
if [[ "$TOPOLOGY" == "small" || "$HOST_COUNT" -le 32 ]]; then
    for i in $(seq 0 $((NUM_PAIRS - 1))); do
        echo "  ${PAIRS_SRC[$i]} (${HOST_IPS[${PAIRS_SRC[$i]}]}) -> ${PAIRS_DST[$i]} (${HOST_IPS[${PAIRS_DST[$i]}]})"
    done
else
    echo "  (pair list suppressed for $TOPOLOGY — $NUM_PAIRS total pairs covering all $HOST_COUNT hosts)"
fi

dump_ovs() {
    local tag="$1"
    local outfile="${OUTDIR}/ovs_${tag}.txt"
    {
        echo "=== OVS Snapshot: $tag @ $(date -Ins) ==="
        local ALL_BRIDGES TOTAL=0
        ALL_BRIDGES="$(ovs-vsctl list-br 2>/dev/null || true)"
        for sw in $ALL_BRIDGES; do
            echo "--- $sw ---"
            ovs-ofctl -O OpenFlow13 dump-flows "$sw" 2>/dev/null || true
            local count
            count="$(ovs-ofctl -O OpenFlow13 dump-flows "$sw" 2>/dev/null | grep -c 'n_packets' 2>/dev/null || echo 0)"
            echo "$sw flow_count=$count"
            TOTAL=$((TOTAL + count))
        done
        echo "TOTAL_FLOWS=$TOTAL"
    } > "$outfile" 2>&1
}

count_total_flows() {
    local total=0 br c
    for br in $(ovs-vsctl list-br 2>/dev/null || true); do
        c="$(ovs-ofctl -O OpenFlow13 dump-flows "$br" 2>/dev/null | grep -c 'n_packets' 2>/dev/null || echo 0)"
        total=$((total + c))
    done
    echo "$total"
}

stop_all_iperf() {
    for hname in "${!HOST_PIDS[@]}"; do
        mnexec -a "${HOST_PIDS[$hname]}" bash -lc "pkill -f '^iperf' 2>/dev/null || true" 2>/dev/null || true
    done
    sleep 1
}

log_step() {
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  $1"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
}

compute_stats() {
    local datafile="$1"
    awk '
    BEGIN { n=0; sum=0; min=1e18; max=-1e18; ss=0 }
    /^[0-9]/ {
        n++; v=$1; sum+=v; vals[n]=v
        if (v < min) min=v
        if (v > max) max=v
    }
    END {
        if (n == 0) { print "  No data"; exit }
        mean = sum / n
        for (i=1; i<=n; i++) ss += (vals[i] - mean)^2
        std  = (n > 1) ? sqrt(ss / (n-1)) : 0
        ci   = 1.96 * std / sqrt(n)
        for (i=1; i<=n; i++) for (j=i+1; j<=n; j++) if (vals[j] < vals[i]) { t=vals[i]; vals[i]=vals[j]; vals[j]=t }
        p50_i = int(0.50*n + 0.5); if (p50_i<1) p50_i=1; if (p50_i>n) p50_i=n
        p95_i = int(0.95*n + 0.5); if (p95_i<1) p95_i=1; if (p95_i>n) p95_i=n
        p99_i = int(0.99*n + 0.5); if (p99_i<1) p99_i=1; if (p99_i>n) p99_i=n
        printf "  Count  : %d\n", n
        printf "  Mean   : %.3f ms\n", mean
        printf "  Std Dev: %.3f ms\n", std
        printf "  95%% CI : +/-%.3f ms\n", ci
        printf "  Min    : %.3f ms\n", min
        printf "  Max    : %.3f ms\n", max
        printf "  p50    : %.3f ms\n", vals[p50_i]
        printf "  p95    : %.3f ms\n", vals[p95_i]
        printf "  p99    : %.3f ms\n", vals[p99_i]
    }' "$datafile"
}

extract_rppt_values() {
    local lines="$1" outfile="$2"
    echo "$lines" | grep -oP 'elapsed_ms=\K[0-9.]+' > "$outfile" || true
}

preflight_controller_tokens() {
    if [[ -z "$RYU_LOG" || ! -f "$RYU_LOG" || "$NUM_PAIRS" -lt 1 ]]; then
        echo "[PREFLIGHT] Skipping token preflight"
        return 0
    fi
    echo "[PREFLIGHT] Verifying controller log tokens..."
    local pre_start pre_lines src dst dst_ip
    pre_start="$(wc -l < "$RYU_LOG" | tr -d ' \n')"
    src="${PAIRS_SRC[0]}"
    dst="${PAIRS_DST[0]}"
    dst_ip="${HOST_IPS[$dst]}"
    mnexec -a "${HOST_PIDS[$dst]}" bash -lc "iperf -s -i 1" >/tmp/baseline_preflight_server.log 2>&1 &
    PREFLIGHT_SRV_PID="$!"
    sleep 1
    mnexec -a "${HOST_PIDS[$src]}" bash -lc "iperf -c ${dst_ip} -t 2 -i 1" >/tmp/baseline_preflight_client.log 2>&1 || true
    sleep 1
    kill "$PREFLIGHT_SRV_PID" 2>/dev/null || true
    stop_all_iperf
    pre_lines="$(tail -n +$((pre_start + 1)) "$RYU_LOG" 2>/dev/null || true)"
    grep -q 'TOPO_DISCOVERY_COMPLETE' "$RYU_LOG" && echo "[PREFLIGHT] OK: TOPO_DISCOVERY_COMPLETE" || echo "[PREFLIGHT][WARN] TOPO_DISCOVERY_COMPLETE missing"
    echo "$pre_lines" | grep -q 'RPPT_MEASURED' && echo "[PREFLIGHT] OK: RPPT_MEASURED" || echo "[PREFLIGHT][WARN] RPPT_MEASURED not seen during warm-up"
    echo "$pre_lines" | grep -q 'FLOW_SETUP' && echo "[PREFLIGHT] OK: FLOW_SETUP" || echo "[PREFLIGHT][WARN] FLOW_SETUP not seen during warm-up"
}

preflight_controller_tokens

RYU_LOG_START=0
if [[ -n "$RYU_LOG" && -f "$RYU_LOG" ]]; then
    RYU_LOG_START="$(wc -l < "$RYU_LOG" | tr -d ' \n')"
    echo "[INIT] Ryu log start: line $RYU_LOG_START"
fi

PIDSTAT_PID=""
if [[ "$HAS_PIDSTAT" -eq 1 && -n "$RYUPID" ]]; then
    TOTAL_DURATION=$(( DUR * 5 + LONG_DUR + CHURN_DUR + 300 ))
    pidstat -rud -h -p "$RYUPID" 1 "$TOTAL_DURATION" > "${OUTDIR}/F_controller_pidstat.txt" 2>&1 &
    PIDSTAT_PID="$!"
    echo "[F] pidstat started (PID=$PIDSTAT_PID, window=${TOTAL_DURATION}s)"
fi

log_step "[1] RFC 8456 §5.1.1 — Topology Discovery Time"
dump_ovs "initial"
{
    echo "=== RFC 8456 §5.1.1 Topology Discovery Time ==="
    echo "Topology : $TOPO_LABEL"
    echo "Timestamp: $(date -Ins)"
    echo ""
    if [[ -n "$RYU_LOG" && -f "$RYU_LOG" ]]; then
        DISCO_LINE="$(grep 'TOPO_DISCOVERY_COMPLETE' "$RYU_LOG" | tail -1 || true)"
        if [[ -n "$DISCO_LINE" ]]; then
            echo "Found: $DISCO_LINE"
            ELAPSED="$(echo "$DISCO_LINE" | grep -oP 'elapsed=\K[0-9.]+' || echo 'N/A')"
            DISCO_SW="$(echo "$DISCO_LINE" | grep -oP 'switches=\K[0-9]+' | head -1 || echo 'N/A')"
            DISCO_LINKS="$(echo "$DISCO_LINE" | grep -oP 'directed_links=\K[0-9]+' || echo 'N/A')"
            DISCO_HOSTS="$(echo "$DISCO_LINE" | grep -oP 'hosts=\K[0-9]+' || echo 'N/A')"
            echo "Discovery elapsed   : ${ELAPSED}s"
            echo "Switches discovered : ${DISCO_SW}"
            echo "Directed links      : ${DISCO_LINKS}"
            echo "Hosts at completion : ${DISCO_HOSTS}"
        else
            echo "TOPO_DISCOVERY_COMPLETE not found in log."
            echo "Token emitted by baseline.py:"
            echo "  TOPO_DISCOVERY_COMPLETE reason=X elapsed=X.XXX switches=N directed_links=N hosts=N"
        fi
        HOST_LEARNED_COUNT="$(grep -c 'HOST_LEARNED:' "$RYU_LOG" 2>/dev/null || echo 0)"
        echo ""
        echo "HOST_LEARNED events in log : ${HOST_LEARNED_COUNT}"
    else
        echo "No Ryu log available."
    fi
    echo ""
    ovs-vsctl show 2>/dev/null || true
} > "${OUTDIR}/1_topology_discovery_time.txt"
cat "${OUTDIR}/1_topology_discovery_time.txt"

log_step "[D+E] UDP Jitter + Loss — Model 1"
stop_all_iperf
dump_ovs "pre_udp"
UDP_PAIR_COUNT="$NUM_PAIRS"
if [[ "${UDP_MAX_PAIRS:-0}" -gt 0 && "$NUM_PAIRS" -gt "$UDP_MAX_PAIRS" ]]; then
    UDP_PAIR_COUNT="$UDP_MAX_PAIRS"
fi
for i in $(seq 0 $((UDP_PAIR_COUNT - 1))); do
    dst="${PAIRS_DST[$i]}"
    mnexec -a "${HOST_PIDS[$dst]}" bash -lc "iperf -s -u -i 1" > "${OUTDIR}/DE_udp_server_${dst}.txt" 2>&1 &
done
sleep 2
UDP_PIDS=()
for i in $(seq 0 $((UDP_PAIR_COUNT - 1))); do
    src="${PAIRS_SRC[$i]}"
    dst="${PAIRS_DST[$i]}"
    dst_ip="${HOST_IPS[$dst]}"
    mnexec -a "${HOST_PIDS[$src]}" bash -lc "iperf -c ${dst_ip} -u -b ${UDP_RATE_PER_PAIR} -t ${DUR} -i 1" > "${OUTDIR}/DE_udp_client_${src}_to_${dst}.txt" 2>&1 &
    UDP_PIDS+=("$!")
    sleep "${UDP_START_STAGGER}"
done
sleep "$((DUR / 2))"
dump_ovs "mid_udp"
for pid in "${UDP_PIDS[@]}"; do wait "$pid" 2>/dev/null || true; done
sleep 2
stop_all_iperf
dump_ovs "post_udp"

JITTER_VALUES_FILE="${OUTDIR}/DE_udp_jitter_values.txt"
LOSS_PCT_VALUES_FILE="${OUTDIR}/DE_udp_loss_pct_values.txt"
: > "$JITTER_VALUES_FILE"
: > "$LOSS_PCT_VALUES_FILE"
SERVER_SUMMARY_OK_COUNT=0
NO_SERVER_SUMMARY_COUNT=0
NO_SUMMARY_COUNT=0
CLIENT_ERROR_COUNT=0
ACK_MISSED_NOTE_COUNT=0
for i in $(seq 0 $((UDP_PAIR_COUNT - 1))); do
    src="${PAIRS_SRC[$i]}"; dst="${PAIRS_DST[$i]}"
    sf="${OUTDIR}/DE_udp_server_${dst}.txt"
    cf="${OUTDIR}/DE_udp_client_${src}_to_${dst}.txt"
    sline="$(grep -E '[0-9.]+-[0-9.]+ sec .* [KMG]?bits/sec .* ms .*\([0-9.]+%\)' "$sf" | tail -1 || true)"
    cline="$(grep -E '[0-9.]+-[0-9.]+ sec .* [KMG]?bits/sec' "$cf" | tail -1 || true)"
    if [[ -n "$sline" ]]; then
        SERVER_SUMMARY_OK_COUNT=$((SERVER_SUMMARY_OK_COUNT + 1))
        jitter="$(echo "$sline" | grep -oP '\s\K[0-9.]+(?= ms\s+[0-9]+/\s*[0-9]+ \()' | tail -1 || true)"
        loss="$(echo "$sline" | grep -oP '(?<=\()[0-9.]+(?=%)' | tail -1 || true)"
        [[ -n "$jitter" ]] && echo "$jitter" >> "$JITTER_VALUES_FILE"
        [[ -n "$loss" ]] && echo "$loss" >> "$LOSS_PCT_VALUES_FILE"
    elif [[ -n "$cline" ]]; then
        NO_SERVER_SUMMARY_COUNT=$((NO_SERVER_SUMMARY_COUNT + 1))
    else
        NO_SUMMARY_COUNT=$((NO_SUMMARY_COUNT + 1))
    fi
    grep -qiE 'connect failed|no route|network is unreachable|invalid argument|broken pipe' "$cf" && CLIENT_ERROR_COUNT=$((CLIENT_ERROR_COUNT + 1)) || true
    grep -qi 'did not receive ack of last datagram after' "$cf" && ACK_MISSED_NOTE_COUNT=$((ACK_MISSED_NOTE_COUNT + 1)) || true
done
{
    echo "=== UDP Jitter + Loss Summary ==="
    echo "Topology : $TOPO_LABEL   Pairs used: $UDP_PAIR_COUNT"
    echo "Server-summary successes : $SERVER_SUMMARY_OK_COUNT / $UDP_PAIR_COUNT"
    [[ -s "$JITTER_VALUES_FILE" ]] && { echo "Mean jitter (ms): $(awk '{s+=$1;n++} END{if(n) printf "%.6f\n", s/n}' "$JITTER_VALUES_FILE")"; }
    [[ -s "$LOSS_PCT_VALUES_FILE" ]] && { echo "Mean loss (%): $(awk '{s+=$1;n++} END{if(n) printf "%.6f\n", s/n}' "$LOSS_PCT_VALUES_FILE")"; }
    echo "NO_SERVER_SUMMARY : $NO_SERVER_SUMMARY_COUNT"
    echo "NO_SUMMARY        : $NO_SUMMARY_COUNT"
    echo "CLIENT_ERROR      : $CLIENT_ERROR_COUNT"
    echo "ACK_MISSED        : $ACK_MISSED_NOTE_COUNT"
} > "${OUTDIR}/DE_udp_summary.txt"
cat "${OUTDIR}/DE_udp_summary.txt"

log_step "[A] ICMP Latency — Model 1 — ${NUM_PAIRS} fixed pairs (${DUR}s)"
PING_COUNT="$((DUR * 5))"
PING_PIDS=()
for i in $(seq 0 $((NUM_PAIRS - 1))); do
    src="${PAIRS_SRC[$i]}"; dst="${PAIRS_DST[$i]}"; dst_ip="${HOST_IPS[$dst]}"
    mnexec -a "${HOST_PIDS[$src]}" bash -lc "ping -i 0.2 -c ${PING_COUNT} ${dst_ip}" > "${OUTDIR}/A_latency_${src}_to_${dst}.txt" 2>&1 &
    PING_PIDS+=("$!")
done
dump_ovs "during_ping"
for pid in "${PING_PIDS[@]}"; do wait "$pid" 2>/dev/null || true; done
{
    echo "=== ICMP Latency Summary (RFC 8456 §5.1.2) ==="
    echo "Topology : $TOPO_LABEL   Pairs: $NUM_PAIRS"
    echo ""
    printf "%-30s %-10s %-10s %-10s %-10s\n" "Pair" "Min(ms)" "Avg(ms)" "Max(ms)" "Loss(%)"
    printf "%-30s %-10s %-10s %-10s %-10s\n" "------------------------------" "--------" "--------" "--------" "--------"
    SUM_AVG=0; N_RTT=0
    for i in $(seq 0 $((NUM_PAIRS - 1))); do
        src="${PAIRS_SRC[$i]}"; dst="${PAIRS_DST[$i]}"; f="${OUTDIR}/A_latency_${src}_to_${dst}.txt"
        rtt="$(grep 'min/avg/max' "$f" 2>/dev/null | awk -F'[=/]' '{printf "%-10s %-10s %-10s", $5, $6, $7}' || echo 'N/A        N/A        N/A')"
        loss="$(grep 'packet loss' "$f" 2>/dev/null | awk '{print $6}' || echo 'N/A')"
        printf "%-30s %s %-10s\n" "${src}->${dst}" "$rtt" "$loss"
        avg_val="$(grep 'min/avg/max' "$f" 2>/dev/null | awk -F'[=/]' '{print $6}' || echo '')"
        if [[ -n "$avg_val" && "$avg_val" =~ ^[0-9.]+$ ]]; then SUM_AVG="$(echo "$SUM_AVG + $avg_val" | bc)"; N_RTT=$((N_RTT + 1)); fi
    done
    echo ""
    if [[ "$N_RTT" -gt 0 ]]; then MEAN_RTT="$(echo "scale=3; $SUM_AVG / $N_RTT" | bc)"; echo "Topology mean RTT : ${MEAN_RTT} ms  (across $N_RTT pairs)"; fi
} > "${OUTDIR}/A_latency_summary.txt"
cat "${OUTDIR}/A_latency_summary.txt"

log_step "[B] TCP Throughput — Model 1"
stop_all_iperf
dump_ovs "pre_tcp"
for i in $(seq 0 $((NUM_PAIRS - 1))); do
    dst="${PAIRS_DST[$i]}"
    mnexec -a "${HOST_PIDS[$dst]}" bash -lc "iperf -s -i 1" > "${OUTDIR}/B_tcp_server_${dst}.txt" 2>&1 &
done
sleep 2
TCP_PIDS=()
for i in $(seq 0 $((NUM_PAIRS - 1))); do
    src="${PAIRS_SRC[$i]}"; dst="${PAIRS_DST[$i]}"; dst_ip="${HOST_IPS[$dst]}"
    mnexec -a "${HOST_PIDS[$src]}" bash -lc "iperf -c ${dst_ip} -t ${DUR} -i 1" > "${OUTDIR}/B_tcp_client_${src}_to_${dst}.txt" 2>&1 &
    TCP_PIDS+=("$!")
done
sleep "$((DUR / 2))"
dump_ovs "mid_tcp"
for pid in "${TCP_PIDS[@]}"; do wait "$pid" 2>/dev/null || true; done
sleep 2
stop_all_iperf
dump_ovs "post_tcp"

TCP_BW_FILE="${OUTDIR}/B_tcp_bw_values.txt"
: > "$TCP_BW_FILE"
for i in $(seq 0 $((NUM_PAIRS - 1))); do
    src="${PAIRS_SRC[$i]}"; dst="${PAIRS_DST[$i]}"
    cf="${OUTDIR}/B_tcp_client_${src}_to_${dst}.txt"
    bw_line="$(grep -E '[0-9.]+-[0-9.]+ sec .* [KMG]?bits/sec' "$cf" | tail -1 || true)"
    if [[ -n "$bw_line" ]]; then
        bw_val="$(echo "$bw_line" | grep -oP '[0-9.]+ [KMG]?bits/sec' | tail -1 | awk '{
            val=$1; unit=$2
            if (unit ~ /^G/) val=val*1000
            else if (unit ~ /^K/) val=val/1000
            print val
        }')"
        [[ -n "$bw_val" ]] && echo "$bw_val" >> "$TCP_BW_FILE"
    fi
done
{
    echo "=== TCP Throughput Summary ==="
    echo "Topology : $TOPO_LABEL   Pairs: $NUM_PAIRS"
    echo ""
    if [[ -s "$TCP_BW_FILE" ]]; then
        awk 'BEGIN{n=0;sum=0;min=1e18;max=-1e18;ss=0}
        /^[0-9]/{
            n++;v=$1;sum+=v;vals[n]=v
            if(v<min) min=v
            if(v>max) max=v
        }
        END{
            if(n==0){print "  No data";exit}
            mean=sum/n
            for(i=1;i<=n;i++) ss+=(vals[i]-mean)^2
            std=(n>1)?sqrt(ss/(n-1)):0
            ci=1.96*std/sqrt(n)
            for(i=1;i<=n;i++) for(j=i+1;j<=n;j++) if(vals[j]<vals[i]){t=vals[i];vals[i]=vals[j];vals[j]=t}
            p50_i=int(0.50*n+0.5); if(p50_i<1)p50_i=1; if(p50_i>n)p50_i=n
            p95_i=int(0.95*n+0.5); if(p95_i<1)p95_i=1; if(p95_i>n)p95_i=n
            p99_i=int(0.99*n+0.5); if(p99_i<1)p99_i=1; if(p99_i>n)p99_i=n
            printf "  Count  : %d pairs\n", n
            printf "  Mean   : %.3f Mbits/sec\n", mean
            printf "  Std Dev: %.3f Mbits/sec\n", std
            printf "  95%% CI : +/-%.3f Mbits/sec\n", ci
            printf "  Min    : %.3f Mbits/sec\n", min
            printf "  Max    : %.3f Mbits/sec\n", max
            printf "  p50    : %.3f Mbits/sec\n", vals[p50_i]
            printf "  p95    : %.3f Mbits/sec\n", vals[p95_i]
            printf "  p99    : %.3f Mbits/sec\n", vals[p99_i]
        }' "$TCP_BW_FILE"
    else
        echo "  No TCP throughput data captured"
    fi
} > "${OUTDIR}/B_tcp_throughput_summary.txt"
cat "${OUTDIR}/B_tcp_throughput_summary.txt"

log_step "[C] TCP Session Continuity — one long-lived flow"
stop_all_iperf
SRC0="${PAIRS_SRC[0]}"; DST0="${PAIRS_DST[0]}"; DST0_IP="${HOST_IPS[$DST0]}"
mnexec -a "${HOST_PIDS[$DST0]}" bash -lc "iperf -s -i 1" > "${OUTDIR}/C_tcp_server_${DST0}.txt" 2>&1 &
C_SRV_PID="$!"
sleep 2
mnexec -a "${HOST_PIDS[$SRC0]}" bash -lc "iperf -c ${DST0_IP} -t ${LONG_DUR} -i 1" > "${OUTDIR}/C_tcp_client_${SRC0}_to_${DST0}.txt" 2>&1 || true
kill "$C_SRV_PID" 2>/dev/null || true
stop_all_iperf
{
    echo "=== TCP Session Continuity Summary ==="
    echo "Topology : $TOPO_LABEL"
    echo "Flow     : ${SRC0} -> ${DST0} (${DST0_IP})"
    if grep -qE '0\.0-.*sec' "${OUTDIR}/C_tcp_client_${SRC0}_to_${DST0}.txt"; then
        echo "RESULT   : survived for ${LONG_DUR}s"
    else
        echo "RESULT   : did not complete cleanly"
    fi
    grep -E '0\.0-|[0-9.]+-[0-9.]+' "${OUTDIR}/C_tcp_client_${SRC0}_to_${DST0}.txt" | tail -5 || true
} > "${OUTDIR}/C_session_continuity_summary.txt"
cat "${OUTDIR}/C_session_continuity_summary.txt"

log_step "[2+N1+N3] Model 2 — Connection Churn (${CHURN_DUR}s, ${CHURN_CONNS} conns/pair)"
stop_all_iperf
dump_ovs "pre_churn"
TOTAL_CHURN_EVENTS=$(( NUM_PAIRS * CHURN_CONNS ))
CHURN_LOG_START=0
if [[ -n "$RYU_LOG" && -f "$RYU_LOG" ]]; then CHURN_LOG_START="$(wc -l < "$RYU_LOG" | tr -d ' \n')"; fi
FLOWS_BEFORE_CHURN="$(count_total_flows)"
T_CHURN_START="$(date +%s%3N)"
for i in $(seq 0 $((NUM_PAIRS - 1))); do
    dst="${PAIRS_DST[$i]}"
    mnexec -a "${HOST_PIDS[$dst]}" bash -lc "iperf -s 2>/dev/null &" 2>/dev/null || true
done
sleep 1
CHURN_PIDS=()
for i in $(seq 0 $((NUM_PAIRS - 1))); do
    src="${PAIRS_SRC[$i]}"; dst_real="${HOST_IPS[${PAIRS_DST[$i]}]}"
    mnexec -a "${HOST_PIDS[$src]}" bash -lc "for j in \$(seq 1 ${CHURN_CONNS}); do iperf -c ${dst_real} -t ${CHURN_CONN_DUR} 2>/dev/null || true; done" > "${OUTDIR}/churn_client_${src}.txt" 2>&1 &
    CHURN_PIDS+=("$!")
done
sleep "$((CHURN_DUR / 2))"
dump_ovs "mid_churn"
for pid in "${CHURN_PIDS[@]}"; do wait "$pid" 2>/dev/null || true; done
T_CHURN_END="$(date +%s%3N)"
ELAPSED_CHURN_MS=$(( T_CHURN_END - T_CHURN_START ))
ELAPSED_CHURN_S="$(echo "scale=3; $ELAPSED_CHURN_MS / 1000" | bc)"
stop_all_iperf
dump_ovs "post_churn"
FLOWS_AFTER_CHURN="$(count_total_flows)"
NEW_FLOW_DELTA=$(( FLOWS_AFTER_CHURN - FLOWS_BEFORE_CHURN ))
CHURN_LINES=""
if [[ -n "$RYU_LOG" && -f "$RYU_LOG" ]]; then CHURN_LINES="$(tail -n +$((CHURN_LOG_START + 1)) "$RYU_LOG" 2>/dev/null || true)"; fi
FLOW_SETUP_COUNT="$(echo "$CHURN_LINES" | grep -c 'FLOW_SETUP' || true)"
RPPT_CHURN_FILE="${OUTDIR}/N3_rppt_values.txt"
extract_rppt_values "$CHURN_LINES" "$RPPT_CHURN_FILE"
{
    echo "=== Async Message Rate / Churn Summary ==="
    echo "Topology        : $TOPO_LABEL"
    echo "Pairs           : $NUM_PAIRS"
    echo "Conns/pair      : $CHURN_CONNS"
    echo "Total attempts  : $TOTAL_CHURN_EVENTS"
    echo "Elapsed         : ${ELAPSED_CHURN_S} s"
    if [[ "$ELAPSED_CHURN_MS" -gt 0 ]]; then
        echo "Approx async rate: $(echo "scale=3; $TOTAL_CHURN_EVENTS / $ELAPSED_CHURN_S" | bc) events/s"
    fi
} > "${OUTDIR}/2_async_rate_summary.txt"
cat "${OUTDIR}/2_async_rate_summary.txt"
{
    echo "=== Flow Setup Rate (N1) ==="
    echo "Flow setups from controller log : $FLOW_SETUP_COUNT"
    echo "OVS flow delta during churn     : $NEW_FLOW_DELTA"
    if [[ "$ELAPSED_CHURN_MS" -gt 0 ]]; then
        echo "Flow setups/sec (log)           : $(echo "scale=3; $FLOW_SETUP_COUNT / $ELAPSED_CHURN_S" | bc)"
        echo "Flow delta/sec (OVS snapshot)   : $(echo "scale=3; $NEW_FLOW_DELTA / $ELAPSED_CHURN_S" | bc)"
    fi
} > "${OUTDIR}/N1_flow_setup_rate.txt"
cat "${OUTDIR}/N1_flow_setup_rate.txt"
{
    echo "=== RPPT Under Load (N3) ==="
    compute_stats "$RPPT_CHURN_FILE"
} > "${OUTDIR}/N3_rppt_under_load.txt"
cat "${OUTDIR}/N3_rppt_under_load.txt"

log_step "[3+N2] RPPT summary (idle + global)"
ALL_LOG_LINES=""
if [[ -n "$RYU_LOG" && -f "$RYU_LOG" ]]; then ALL_LOG_LINES="$(tail -n +$((RYU_LOG_START + 1)) "$RYU_LOG" 2>/dev/null || true)"; fi
RPPT_VALUES_FILE="${OUTDIR}/3_rppt_values.txt"
extract_rppt_values "$ALL_LOG_LINES" "$RPPT_VALUES_FILE"
{
    echo "=== RPPT Summary (RFC 8456 §5.1.4 + N2 percentiles) ==="
    compute_stats "$RPPT_VALUES_FILE"
} > "${OUTDIR}/3_rppt_summary.txt"
cat "${OUTDIR}/3_rppt_summary.txt"

log_step "[4] Forwarding Table Capacity / Snapshots"
{
    echo "=== Forwarding Table Capacity Summary ==="
    echo "Initial total flows : $(grep 'TOTAL_FLOWS=' "${OUTDIR}/ovs_initial.txt" | tail -1 | cut -d= -f2)"
    echo "Mid UDP total flows : $(grep 'TOTAL_FLOWS=' "${OUTDIR}/ovs_mid_udp.txt" | tail -1 | cut -d= -f2)"
    echo "Mid TCP total flows : $(grep 'TOTAL_FLOWS=' "${OUTDIR}/ovs_mid_tcp.txt" | tail -1 | cut -d= -f2)"
    echo "Mid churn flows     : $(grep 'TOTAL_FLOWS=' "${OUTDIR}/ovs_mid_churn.txt" | tail -1 | cut -d= -f2)"
} > "${OUTDIR}/4_forwarding_table_capacity.txt"
cat "${OUTDIR}/4_forwarding_table_capacity.txt"

log_step "[5] NRT — simple link failure / recovery"
NRT_RESULT_FILE="${OUTDIR}/5_nrt_timing.txt"
{
    echo "=== Network Repair Time (best-effort) ==="
    echo "Topology : $TOPO_LABEL"
    BRIDGES=( $(ovs-vsctl list-br 2>/dev/null | sort -V) )
    if [[ "${#BRIDGES[@]}" -lt 2 ]]; then
        echo "NRT unavailable: need at least 2 bridges"
    else
        SRCN="${PAIRS_SRC[0]}"; DSTN="${PAIRS_DST[0]}"; DSTN_IP="${HOST_IPS[$DSTN]}"
        CORE="${BRIDGES[0]}"
        PORT_LINE="$(ovs-ofctl -O OpenFlow13 show "$CORE" 2>/dev/null | grep -m1 'agg' || true)"
        PORT_NO="$(echo "$PORT_LINE" | awk -F'[()]' '{print $2}' | awk -F: '{print $1}' | tr -d ' ' || true)"
        if [[ -z "$PORT_NO" ]]; then
            echo "NRT unavailable: could not infer a test port on $CORE"
        else
            echo "Using bridge=$CORE port=$PORT_NO for transient failure"
            mnexec -a "${HOST_PIDS[$SRCN]}" bash -lc "ping -i 0.2 -c 40 ${DSTN_IP}" > "${OUTDIR}/5_nrt_ping.txt" 2>&1 &
            PING_NRT_PID="$!"
            sleep 2
            T0="$(date +%s%3N)"
            ovs-ofctl mod-port "$CORE" "$PORT_NO" down 2>/dev/null || true
            sleep 2
            ovs-ofctl mod-port "$CORE" "$PORT_NO" up 2>/dev/null || true
            wait "$PING_NRT_PID" 2>/dev/null || true
            T1="$(date +%s%3N)"
            echo "Failure window applied for ~2s"
            echo "Wall-clock event duration: $((T1 - T0)) ms"
            grep 'packet loss' "${OUTDIR}/5_nrt_ping.txt" || true
        fi
    fi
} > "$NRT_RESULT_FILE"
cat "$NRT_RESULT_FILE"

log_step "[F] Controller CPU / Memory summary"
if [[ -n "$PIDSTAT_PID" ]]; then
    wait "$PIDSTAT_PID" 2>/dev/null || true
fi
{
    echo "=== Controller Overhead Summary ==="
    if [[ -f "${OUTDIR}/F_controller_pidstat.txt" ]]; then
        awk '
            /^[0-9]/ && $0 !~ /UID/ {
                cpu += $(NF-6); rss += $(NF-1); n++
                if ($(NF-6) > cpu_max) cpu_max = $(NF-6)
            }
            END {
                if (n == 0) { print "No pidstat samples parsed"; exit }
                printf "Samples              : %d\n", n
                printf "Average CPU (%%)      : %.3f\n", cpu / n
                printf "Peak CPU (%%)         : %.3f\n", cpu_max
                printf "Average RSS (KB)     : %.3f\n", rss / n
                printf "Average RSS (MB)     : %.3f\n", (rss / n) / 1024.0
            }
        ' "${OUTDIR}/F_controller_pidstat.txt"
    else
        echo "pidstat unavailable"
    fi
} > "${OUTDIR}/F_controller_overhead_summary.txt"
cat "${OUTDIR}/F_controller_overhead_summary.txt"

N4_LOG="${OUTDIR}/N4_flow_setup_latency.txt"
{
    echo "=== Flow Setup Latency (N4) ==="
    echo "Topology : $TOPO_LABEL"
    echo "Source   : Ryu log token  FLOW_SETUP: key=(...) pathsw=N flowmods=N elapsed_ms=X.XXX"
    echo ""
    if [[ -n "$RYU_LOG" && -f "$RYU_LOG" ]]; then
        FS_LINES="$(tail -n +$((RYU_LOG_START + 1)) "$RYU_LOG" 2>/dev/null | grep 'FLOW_SETUP:' || true)"
        FS_COUNT="$(echo "$FS_LINES" | grep -c 'FLOW_SETUP:' 2>/dev/null || echo 0)"
        echo "FLOW_SETUP events this run: $FS_COUNT"
        echo ""
        if [[ "$FS_COUNT" -gt 0 ]]; then
            FS_ELAPSED="${OUTDIR}/N4_fs_elapsed_values.txt"
            FS_PATHSW="${OUTDIR}/N4_fs_pathsw_values.txt"
            FS_FLOWMODS="${OUTDIR}/N4_fs_flowmods_values.txt"
            echo "$FS_LINES" | grep -oP 'elapsed_ms=\K[0-9.]+'  > "$FS_ELAPSED"  || true
            echo "$FS_LINES" | grep -oP 'pathsw=\K[0-9]+'       > "$FS_PATHSW"   || true
            echo "$FS_LINES" | grep -oP 'flowmods=\K[0-9]+'     > "$FS_FLOWMODS" || true
            echo "Flow setup elapsed_ms statistics:"
            compute_stats "$FS_ELAPSED"
            echo ""
            AVG_PATHSW="$(awk 'BEGIN{n=0;s=0} /^[0-9]/{n++;s+=$1} END{if(n>0) printf "%.2f",s/n; else print "N/A"}' "$FS_PATHSW")"
            AVG_FLOWMODS="$(awk 'BEGIN{n=0;s=0} /^[0-9]/{n++;s+=$1} END{if(n>0) printf "%.2f",s/n; else print "N/A"}' "$FS_FLOWMODS")"
            echo "Avg path length (pathsw)  : $AVG_PATHSW switches"
            echo "Avg flow mods per setup   : $AVG_FLOWMODS"
        else
            echo "No FLOW_SETUP events found in this run window."
        fi
    else
        echo "Ryu log not available."
    fi
} > "$N4_LOG"
cat "$N4_LOG"

{
    echo "=== Scalability Snapshot (N5) ==="
    echo "Topology         : $TOPO_LABEL"
    echo "Hosts discovered : $FOUND"
    echo "Pairs            : $NUM_PAIRS"
    echo "Churn attempts   : $TOTAL_CHURN_EVENTS"
    echo "Initial flows    : $(grep 'TOTAL_FLOWS=' "${OUTDIR}/ovs_initial.txt" | tail -1 | cut -d= -f2)"
    echo "Mid-churn flows  : $(grep 'TOTAL_FLOWS=' "${OUTDIR}/ovs_mid_churn.txt" | tail -1 | cut -d= -f2)"
    if [[ -s "$RPPT_VALUES_FILE" ]]; then
        echo "RPPT samples     : $(wc -l < "$RPPT_VALUES_FILE" | tr -d ' ')"
    fi
    if [[ -f "${OUTDIR}/N4_fs_pathsw_values.txt" ]]; then
        AVG_PL="$(awk 'BEGIN{n=0;s=0} /^[0-9]/{n++;s+=$1} END{if(n>0) printf "%.2f",s/n; else print "N/A"}' "${OUTDIR}/N4_fs_pathsw_values.txt")"
        AVG_FM="$(awk 'BEGIN{n=0;s=0} /^[0-9]/{n++;s+=$1} END{if(n>0) printf "%.2f",s/n; else print "N/A"}' "${OUTDIR}/N4_fs_flowmods_values.txt")"
        echo "Avg path length  : $AVG_PL switches"
        echo "Avg flow mods    : $AVG_FM per setup"
    fi
} > "${OUTDIR}/N5_scalability_snapshot.txt"
cat "${OUTDIR}/N5_scalability_snapshot.txt"

log_step "FINAL — All-Metrics Summary"
{
    echo "=========================================================="
    echo "=== FINAL SUMMARY — ALL METRICS ==="
    echo "=========================================================="
    echo "Topology  : $TOPO_LABEL"
    echo "Pairs     : $NUM_PAIRS"
    echo "Duration  : ${DUR}s"
    echo ""
    echo "--- [1] Topology Discovery ---"
    grep -E 'Discovery elapsed|Switches discovered|Directed links|Hosts at completion' "${OUTDIR}/1_topology_discovery_time.txt" 2>/dev/null | head -5 || echo "  (no data)"
    echo ""
    echo "--- [A] ICMP Latency ---"
    grep -E 'Topology mean RTT' "${OUTDIR}/A_latency_summary.txt" 2>/dev/null || echo "  (no data)"
    echo ""
    echo "--- [B] TCP Throughput ---"
    grep -E 'Count|Mean|Std Dev|p50|p95|p99' "${OUTDIR}/B_tcp_throughput_summary.txt" 2>/dev/null | head -7 || echo "  (no data)"
    echo ""
    echo "--- [C] TCP Session Continuity ---"
    grep -E 'RESULT|Flow' "${OUTDIR}/C_session_continuity_summary.txt" 2>/dev/null | head -3 || echo "  (no data)"
    echo ""
    echo "--- [D+E] UDP Jitter + Loss ---"
    grep -v '^===$' "${OUTDIR}/DE_udp_summary.txt" 2>/dev/null | grep -v '^$' || echo "  (no data)"
    echo ""
    echo "--- [F] Controller Overhead ---"
    grep -E 'Samples|Average CPU|Peak CPU|Average RSS' "${OUTDIR}/F_controller_overhead_summary.txt" 2>/dev/null | head -4 || echo "  (no data)"
    echo ""
    echo "--- [2] Async Message Rate ---"
    grep -E 'Approx async rate|Elapsed' "${OUTDIR}/2_async_rate_summary.txt" 2>/dev/null | head -3 || echo "  (no data)"
    echo ""
    echo "--- [3+N2] RPPT Summary ---"
    grep -E 'Count|Mean|Std Dev|p50|p95|p99' "${OUTDIR}/3_rppt_summary.txt" 2>/dev/null | head -7 || echo "  (no data)"
    echo ""
    echo "--- [4] Forwarding Table ---"
    grep -v '^===' "${OUTDIR}/4_forwarding_table_capacity.txt" 2>/dev/null | grep -v '^$' || echo "  (no data)"
    echo ""
    echo "--- [5] NRT ---"
    grep -E 'packet loss|Wall-clock|Failure window' "${OUTDIR}/5_nrt_timing.txt" 2>/dev/null | head -3 || echo "  (no data)"
    echo ""
    echo "--- [N1] Flow Setup Rate ---"
    grep -v '^===' "${OUTDIR}/N1_flow_setup_rate.txt" 2>/dev/null | grep -v '^$' | head -5 || echo "  (no data)"
    echo ""
    echo "--- [N3] RPPT Under Load ---"
    grep -E 'Count|Mean|Std Dev|p50|p95|p99' "${OUTDIR}/N3_rppt_under_load.txt" 2>/dev/null | head -7 || echo "  (no data)"
    echo ""
    echo "--- [N4] Flow Setup Latency ---"
    grep -E 'FLOW_SETUP events|Count|Mean|Std Dev|p50|p95|p99' "${OUTDIR}/N4_flow_setup_latency.txt" 2>/dev/null | head -8 || echo "  (no data)"
    echo ""
    echo "--- [N5] Scalability ---"
    grep -v '^===' "${OUTDIR}/N5_scalability_snapshot.txt" 2>/dev/null | grep -v '^$' || echo "  (no data)"
    echo "=========================================================="
} > "${OUTDIR}/FINAL_all_metrics_summary.txt"
cat "${OUTDIR}/FINAL_all_metrics_summary.txt"

cat > "${OUTDIR}/README_results.txt" <<EOF
Baseline benchmark output directory: ${OUTDIR}

Key files:
  [1]    Topology Discovery  : 1_topology_discovery_time.txt
  [2]    Async Msg Rate      : 2_async_rate_summary.txt
  [3+N2] RPPT Summary        : 3_rppt_summary.txt
  [4]    Forwarding Table    : 4_forwarding_table_capacity.txt
  [5]    NRT                 : 5_nrt_timing.txt
  [A]    ICMP Latency        : A_latency_summary.txt
  [B]    TCP Throughput      : B_tcp_throughput_summary.txt
  [C]    TCP Continuity      : C_session_continuity_summary.txt
  [D+E]  UDP Jitter+Loss     : DE_udp_summary.txt
  [F]    Controller Overhead : F_controller_overhead_summary.txt
  [N1]   Flow Setup Rate     : N1_flow_setup_rate.txt
  [N3]   RPPT Under Load     : N3_rppt_under_load.txt
  [N4]   Flow Setup Latency  : N4_flow_setup_latency.txt
  [N5]   Scalability         : N5_scalability_snapshot.txt
  FINAL  All Metrics         : FINAL_all_metrics_summary.txt
EOF

