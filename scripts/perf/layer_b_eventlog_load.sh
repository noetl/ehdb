#!/usr/bin/env bash
# Layer B — in-cluster (kind) event-log load harness for the EHDB
# perf/load-testing workstream (noetl/ehdb#261).
#
# Companion to the Layer A engine micro-benchmarks
# (`cargo bench -p ehdb-reference --bench engine_micro`).  Where Layer A
# isolates the engine's own append/replay cost on the host, Layer B drives
# *real traffic* through the deployed worker in the local kind cluster and
# reads the live `noetl_ehdb_eventlog_*` runtime metrics + the incumbent
# server `noetl_event_ingest_duration_seconds` histogram.
#
# DIRECTIONAL ONLY.  The podman-machine VM backing kind on developer
# hardware is resource-constrained and its PVC fsync path is slow and
# noisy.  These numbers validate *shape and behaviour* ("does the mirror
# fire, do metrics advance, does the segment rotate, does the backlog
# drain, which write path is heavier") — NOT authoritative peak
# throughput.  For engine-cost claims, Layer A wins.
#
# Two write paths are measured on the same VM, same event-envelope shape:
#   (a) INCUMBENT  — server POST /api/events (Postgres INSERT + NATS
#       publish).  `step.enter` is in `skip_engine_events` so the handler
#       is a pure write-boundary op.  Driven with ApacheBench; p50/p95/p99
#       come from the server histogram.  This is the log-and-store path
#       EHDB's durable event-log tier is designed to replace.
#   (b) EHDB       — the worker's durable_segment shadow mirror, fired by
#       real playbook drives.  Throughput = mirror-counter delta / wall;
#       latency = high-frequency samples of the last-op-duration gauge.
#
# NOTE on the EHDB shadow config in this cluster: the worker runs the
# *shared-tier* durable driver (SharedDurableEventLogDriver) — every append
# writes the local segment AND re-publishes the active segment to the
# shared store.  That is heavier than the local-only DurableEventLogDriver
# that Layer A benched.  The harness reports the mirror latency as-deployed
# and the interpretation on the wiki separates the two backends.
#
# Usage:
#   scripts/perf/layer_b_eventlog_load.sh all
#   scripts/perf/layer_b_eventlog_load.sh incumbent-sustained
#   scripts/perf/layer_b_eventlog_load.sh incumbent-burst
#   scripts/perf/layer_b_eventlog_load.sh ehdb-drive
#
# Tunables (env):
#   KCTX=kind-noetl NS=noetl
#   SUS_TOTAL=20000 SUS_CONC=50        # incumbent sustained AB shape
#   BURST_TOTAL=8000 BURST_CONC=200    # incumbent burst AB shape
#   DRIVE_PLAYBOOK=fixtures/playbooks/hello_world
#   DRIVE_WAVES=6 DRIVE_WIDTH=12       # ehdb: WAVES rounds of WIDTH concurrent drives
#   SAMPLE_MS=250                      # ehdb metric-gauge sample interval

set -uo pipefail

MODE=${1:-all}

KCTX=${KCTX:-kind-noetl}
NS=${NS:-noetl}
SRV_SVC=${SRV_SVC:-noetl-server-rust}
SRV_PORT=${SRV_PORT:-38082}
WORKER_LABEL=${WORKER_LABEL:-app=noetl-worker-rust}
WORKER_METRICS_PORT=${WORKER_METRICS_PORT:-9091}
NATS_MON_PORT=${NATS_MON_PORT:-8222}

SUS_TOTAL=${SUS_TOTAL:-20000}
SUS_CONC=${SUS_CONC:-50}
BURST_TOTAL=${BURST_TOTAL:-8000}
BURST_CONC=${BURST_CONC:-200}
TEST_PLAYBOOK_PATH=${TEST_PLAYBOOK_PATH:-fixtures/playbooks/hello_world}
DRIVE_PLAYBOOK=${DRIVE_PLAYBOOK:-fixtures/playbooks/hello_world}
DRIVE_WAVES=${DRIVE_WAVES:-6}
DRIVE_WIDTH=${DRIVE_WIDTH:-12}
SAMPLE_MS=${SAMPLE_MS:-250}

OUT=${OUT:-/tmp/ehdb-layer-b}
mkdir -p "$OUT"

cyan(){ printf '\033[36m%s\033[0m' "$1"; }
green(){ printf '\033[32m%s\033[0m' "$1"; }
yellow(){ printf '\033[33m%s\033[0m' "$1"; }
red(){ printf '\033[31m%s\033[0m' "$1"; }
step(){ printf '\n%s %s\n' "$(cyan '==>')" "$1"; }
ok(){ printf '    %s %s\n' "$(green PASS)" "$1"; }
warn(){ printf '    %s %s\n' "$(yellow WARN)" "$1"; }
fail(){ printf '    %s %s\n' "$(red FAIL)" "$1"; }

PIDS=()
cleanup(){ for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT

# --- discovery -------------------------------------------------------------
command -v ab >/dev/null 2>&1 || { fail "ApacheBench (ab) missing — brew install httpd"; exit 1; }
command -v jq >/dev/null 2>&1 || { fail "jq missing"; exit 1; }
WORKER_POD=$(kubectl --context "$KCTX" -n "$NS" get pods -l "$WORKER_LABEL" \
  --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
[ -n "$WORKER_POD" ] || { fail "no running worker pod for $WORKER_LABEL"; exit 1; }
ok "worker pod: $WORKER_POD"

# --- port-forwards ---------------------------------------------------------
# Pre-clear any stale listeners on our ports (leaked PFs from a prior run
# leave curl hanging against a half-open forwarder).
for _p in "$SRV_PORT" "$WORKER_METRICS_PORT" "$NATS_MON_PORT"; do
  for _pid in $(lsof -ti tcp:"$_p" 2>/dev/null); do kill "$_pid" 2>/dev/null || true; done
done
sleep 1
kubectl --context "$KCTX" -n "$NS" port-forward "svc/$SRV_SVC" "$SRV_PORT:8082" >"$OUT/srv-pf.log" 2>&1 &
PIDS+=($!)
kubectl --context "$KCTX" -n "$NS" port-forward "$WORKER_POD" "$WORKER_METRICS_PORT:9090" >"$OUT/wk-pf.log" 2>&1 &
PIDS+=($!)
kubectl --context "$KCTX" -n nats port-forward svc/nats "$NATS_MON_PORT:8222" >"$OUT/nats-pf.log" 2>&1 &
PIDS+=($!)
for i in $(seq 1 25); do
  curl -s --max-time 5 -o /dev/null -w '%{http_code}' "http://localhost:$SRV_PORT/api/health" 2>/dev/null | grep -q 200 && break
  sleep 1
done
ok "port-forwards up (srv:$SRV_PORT worker-metrics:$WORKER_METRICS_PORT nats:$NATS_MON_PORT)"

srv_metrics(){ curl -s --max-time 15 "http://localhost:$SRV_PORT/metrics" 2>/dev/null; }
wk_metrics(){ curl -s --max-time 8 "http://localhost:$WORKER_METRICS_PORT/metrics" 2>/dev/null; }
mirror_count(){ wk_metrics | awk '/noetl_ehdb_eventlog_ops_total.*operation="mirror".*mirrored/{print $2}'; }
last_dur(){ wk_metrics | awk '/noetl_ehdb_eventlog_last_duration_seconds/&&!/#/{print $2}'; }
cmd_pending(){ curl -s --max-time 8 "http://localhost:$NATS_MON_PORT/jsz?streams=true&consumers=true" 2>/dev/null \
  | python3 -c "import sys,json;d=json.load(sys.stdin);print(sum(c.get('num_pending',0) for a in d.get('account_details',[]) for s in a.get('stream_detail',[]) if s['name']=='NOETL_COMMANDS' for c in s.get('consumer_detail',[])))" 2>/dev/null; }
seg_state(){ kubectl --context "$KCTX" -n "$NS" exec "$WORKER_POD" -c worker -- sh -c \
  'echo "local_segs=$(ls /ehdb-durable/local/shard-0000 2>/dev/null | grep -c eslog) local_bytes=$(du -sk /ehdb-durable/local 2>/dev/null | cut -f1) shared_segs=$(ls /ehdb-durable/shared 2>/dev/null | grep -c "seg\.[0-9]*\.[0-9]*$") shared_bytes=$(du -sk /ehdb-durable/shared 2>/dev/null | cut -f1)"' 2>/dev/null; }

# linear-interpolated quantiles over the DELTA of a Prometheus cumulative
# histogram between two snapshots (mirrors histogram_quantile on the run's
# own events, so repeated runs on one pod don't blend).
hist_pct(){ # $1=before-file $2=after-file $3=event_type
  python3 - "$1" "$2" "$3" <<'PY'
import sys,re
bf,af,et=sys.argv[1],sys.argv[2],sys.argv[3]
def load(f):
    b={};cnt=0.0;sm=0.0
    for line in open(f):
        m=re.match(r'noetl_event_ingest_duration_seconds_bucket\{event_type="%s",le="([^"]+)"\}\s+([0-9.]+)'%re.escape(et),line)
        if m: b[float('inf') if m.group(1)=='+Inf' else float(m.group(1))]=float(m.group(2))
        m2=re.match(r'noetl_event_ingest_duration_seconds_count\{event_type="%s"\}\s+([0-9.]+)'%re.escape(et),line)
        if m2: cnt=float(m2.group(1))
        m3=re.match(r'noetl_event_ingest_duration_seconds_sum\{event_type="%s"\}\s+([0-9.]+)'%re.escape(et),line)
        if m3: sm=float(m3.group(1))
    return b,cnt,sm
bb,bc,bs=load(bf); ab,ac,as_=load(af)
les=sorted(ab); delta=[(le,ab[le]-bb.get(le,0)) for le in les]
total=delta[-1][1] if delta else 0; dsum=as_-bs
def q(p):
    if total<=0: return None
    rank=p*total; prev_le=0.0; prev_c=0.0
    for le,c in delta:
        if c>=rank:
            if le==float('inf'): return prev_le
            if c==prev_c: return le
            return prev_le+(le-prev_le)*((rank-prev_c)/(c-prev_c))
        prev_le,prev_c=le,c
    return delta[-1][0]
def ms(x): return "n/a" if x is None else f"{x*1000:.2f}ms"
mean=(dsum/total*1000) if total>0 else None
print(f"count={int(total)} mean={('%.2fms'%mean) if mean else 'n/a'} p50={ms(q(.50))} p95={ms(q(.95))} p99={ms(q(.99))}")
PY
}

pct_of_samples(){ # $1=samples-file -> dedup consecutive (~1 per real append) + percentiles
  python3 - "$1" <<'PY'
import sys
raw=[l.strip() for l in open(sys.argv[1]) if l.strip()]
# dedup consecutive identical gauge reads: each distinct run ~= one append
dedup=[];prev=None
for v in raw:
    if v!=prev: dedup.append(float(v))
    prev=v
xs=sorted(dedup)
if not xs:
    print("no samples"); sys.exit(0)
def q(p):
    i=min(len(xs)-1,int(round(p*(len(xs)-1)))); return xs[i]
def ms(x): return f"{x*1000:.1f}ms"
print(f"distinct-appends={len(xs)} (raw={len(raw)}) min={ms(xs[0])} p50={ms(q(.5))} p95={ms(q(.95))} p99={ms(q(.99))} max={ms(xs[-1])}")
PY
}

alloc_exec(){ # returns execution_id for a synthetic parent (incumbent AB needs a valid parent)
  curl -s --max-time 20 -X POST -H 'Content-Type: application/json' \
    -d "{\"path\":\"$TEST_PLAYBOOK_PATH\",\"payload\":{}}" \
    "http://localhost:$SRV_PORT/api/execute" 2>/dev/null | jq -r '.execution_id // empty'
}

incumbent_run(){ # $1=label $2=total $3=conc
  local label=$1 total=$2 conc=$3
  step "INCUMBENT [$label]  POST /api/events  n=$total c=$conc  (Postgres+NATS append)"
  local eid; eid=$(alloc_exec)
  [ -n "$eid" ] || { fail "no execution_id"; return 1; }
  cat >"$OUT/payload.json" <<EOF
{"execution_id":"$eid","step":"load_smoke","event_type":"step.enter","result_kind":"data","payload":{"status":"OK"},"actionable":false,"informative":true}
EOF
  srv_metrics >"$OUT/inc-before.txt"
  local t0 t1; t0=$(perl -e 'print time')
  ab -n "$total" -c "$conc" -k -p "$OUT/payload.json" -T 'application/json' \
    "http://localhost:$SRV_PORT/api/events" >"$OUT/ab-$label.out" 2>&1
  t1=$(perl -e 'print time')
  srv_metrics >"$OUT/inc-after.txt"
  local wall=$((t1-t0)); [ "$wall" -lt 1 ] && wall=1
  local before after delta
  before=$(grep '^noetl_events_ingested_total' "$OUT/inc-before.txt" | awk -F'} ' '{s+=$2}END{print s+0}')
  after=$(grep '^noetl_events_ingested_total' "$OUT/inc-after.txt" | awk -F'} ' '{s+=$2}END{print s+0}')
  delta=$((after-before))
  local rps; rps=$(grep 'Requests per second' "$OUT/ab-$label.out" | awk '{print $4}')
  local failed; failed=$(grep 'Failed requests' "$OUT/ab-$label.out" | awk '{print $3}')
  local non2xx; non2xx=$(grep 'Non-2xx' "$OUT/ab-$label.out" | awk '{print $NF}'); non2xx=${non2xx:-0}
  echo "    ab: ${rps} req/s | failed=${failed} non2xx=${non2xx} | ingested_delta=${delta} over ${wall}s (=$((delta/wall)) ev/s)"
  echo -n "    server-histogram step.enter (this run only): "; hist_pct "$OUT/inc-before.txt" "$OUT/inc-after.txt" "step.enter"
  printf '%s\t%s req/s\t%s ev/s(ingest)\t%s\n' "$label" "${rps:-?}" "$((delta/wall))" "$(hist_pct "$OUT/inc-before.txt" "$OUT/inc-after.txt" step.enter)" >>"$OUT/incumbent-summary.tsv"
}

ehdb_drive_run(){
  step "EHDB durable_segment shadow mirror — $DRIVE_WAVES waves x $DRIVE_WIDTH concurrent drives of $DRIVE_PLAYBOOK"
  local c0 c1 t0 t1
  echo "    seg BEFORE: $(seg_state)"
  echo "    backlog BEFORE: pending=$(cmd_pending)"
  c0=$(mirror_count); t0=$(perl -e 'print time')
  : >"$OUT/ehdb-lastdur.samples"
  # background sampler of the last-op-duration gauge
  ( while :; do d=$(last_dur); [ -n "$d" ] && echo "$d" >>"$OUT/ehdb-lastdur.samples"; perl -e "select(undef,undef,undef,$SAMPLE_MS/1000.0)"; done ) &
  local SAMP=$!
  local peak_pending=0
  for w in $(seq 1 "$DRIVE_WAVES"); do
    local dpids=()
    for d in $(seq 1 "$DRIVE_WIDTH"); do
      curl -s --max-time 20 -X POST -H 'Content-Type: application/json' \
        -d "{\"path\":\"$DRIVE_PLAYBOOK\",\"payload\":{}}" \
        "http://localhost:$SRV_PORT/api/execute" >/dev/null 2>&1 &
      dpids+=($!)
    done
    # wait ONLY on the drive curls, not the infinite background sampler
    wait "${dpids[@]}"
    local p; p=$(cmd_pending); [ "${p:-0}" -gt "$peak_pending" ] && peak_pending=$p
    printf '    wave %s/%s dispatched (backlog pending=%s, mirror=%s)\n' "$w" "$DRIVE_WAVES" "${p:-?}" "$(mirror_count)"
  done
  # let the tail of events drain + mirror (capped — the synchronous shared-tier
  # mirror throttles emission, so full drain can take minutes; we report the
  # backlog behaviour rather than block on it)
  local drain=0 drained=1
  while :; do
    p=$(cmd_pending); [ "${p:-0}" -eq 0 ] && { sleep 3; p2=$(cmd_pending); [ "${p2:-0}" -eq 0 ] && break; }
    drain=$((drain+3)); [ "$drain" -gt "${DRAIN_CAP:-60}" ] && { warn "drain cap hit at pending=$p (worker throttled by synchronous mirror)"; drained=0; break; }
    sleep 3
  done
  kill "$SAMP" 2>/dev/null || true
  c1=$(mirror_count); t1=$(perl -e 'print time')
  local wall=$((t1-t0)); [ "$wall" -lt 1 ] && wall=1
  local mdelta=$((c1-c0))
  echo "    seg AFTER:  $(seg_state)"
  echo "    mirror ops: $mdelta over ${wall}s  => $(python3 -c "print(f'{$mdelta/$wall:.1f}')") mirror-append/s (serialized single-writer)"
  echo "    peak backlog pending during load: $peak_pending"
  echo -n "    mirror-append latency (gauge samples): "
  pct_of_samples "$OUT/ehdb-lastdur.samples"
}

# Decompose the as-deployed shared-tier append cost on the SAME VM using the
# in-image ehdb-selfcheck binary: appends to a FRESH empty segment (local
# fsync + tiny shared publish) vs the LIVE shard (whole active-segment
# re-publish).  Proves the O(active-segment-size) shared-publish attribution.
ehdb_decompose(){
  step "EHDB append decomposition (in-VM, ehdb-selfcheck durable-eventlog)"
  local SC=/app/ehdb-selfcheck
  echo "    A) fresh empty segment (local durable append primitive):"
  for r in 1 2 3; do
    v=$(kubectl --context "$KCTX" -n "$NS" exec "$WORKER_POD" -c worker -- sh -c \
      "D=/tmp/sc-fresh-$r; rm -rf \$D; mkdir -p \$D/shared; NOETL_EHDB_EVENTLOG_DURABLE_DIR=\$D NOETL_EHDB_EVENTLOG_SHARED_DIR=\$D/shared $SC durable-eventlog 2>&1 | awk '/noetl_ehdb_eventlog_last_duration_seconds /&&!/#/{print \$2}'" 2>/dev/null)
    echo "       run $r: ${v}s"
  done
  echo "    B) live shard (as-deployed shared-tier, whole active-segment re-publish):"
  for r in 1 2 3; do
    v=$(kubectl --context "$KCTX" -n "$NS" exec "$WORKER_POD" -c worker -- sh -c \
      "$SC durable-eventlog 2>&1 | awk '/noetl_ehdb_eventlog_last_duration_seconds /&&!/#/{print \$2}'" 2>/dev/null)
    echo "       run $r: ${v}s"
  done
  echo "    => fresh≈local-engine floor (corroborates Layer A); live≈floor+O(active-seg) shared re-publish on VM PVC."
}

report(){
  step "SUMMARY"
  echo "  --- incumbent (server POST /api/events, Postgres+NATS) ---"
  [ -f "$OUT/incumbent-summary.tsv" ] && column -t -s$'\t' "$OUT/incumbent-summary.tsv" | sed 's/^/    /'
  echo "  --- EHDB durable_segment shadow mirror (worker, shared-tier) ---"
  echo "    see ehdb-drive output above"
  echo
  warn "DIRECTIONAL — podman-VM PVC fsync is slow+noisy; Layer A micro-benches are the authoritative engine signal."
}

case "$MODE" in
  incumbent-sustained) : >"$OUT/incumbent-summary.tsv"; incumbent_run sustained "$SUS_TOTAL" "$SUS_CONC" ;;
  incumbent-burst)     : >"$OUT/incumbent-summary.tsv"; incumbent_run burst "$BURST_TOTAL" "$BURST_CONC" ;;
  ehdb-drive)          ehdb_drive_run ;;
  ehdb-decompose)      ehdb_decompose ;;
  all)
    : >"$OUT/incumbent-summary.tsv"
    incumbent_run sustained "$SUS_TOTAL" "$SUS_CONC"
    incumbent_run burst "$BURST_TOTAL" "$BURST_CONC"
    ehdb_decompose
    ehdb_drive_run
    report ;;
  *) fail "unknown mode: $MODE (want: all|incumbent-sustained|incumbent-burst|ehdb-drive|ehdb-decompose)"; exit 1 ;;
esac

step "Done."
