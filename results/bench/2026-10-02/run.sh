#!/usr/bin/env bash
# Usage: run.sh <label> <first-round> <last-round> "<rate> <duration> <mode>" ...
# One fresh database + fresh jobrund per run; specs interleaved within each round.
set -u
S=${BENCH_DIR:-./bench-work}   # needs $S/bin/jobrund(.exe) and $S/bin/jobrun(.exe)
LABEL=$1; FIRST=$2; ROUNDS=$3; shift 3; SPECS=("$@")
OUT=$S/out; mkdir -p "$OUT"
for r in $(seq "$FIRST" "$ROUNDS"); do
  for spec in "${SPECS[@]}"; do
    read -r RATE DUR MODE <<<"$spec"
    NAME="${LABEL}_r${r}_${RATE}_${MODE}"
    DB="jb_${LABEL}_r${r}_${RATE}_${MODE}"
    docker exec jobrun-postgres-1 psql -U jobrun -d postgres -qc "drop database if exists $DB" >/dev/null
    docker exec jobrun-postgres-1 psql -U jobrun -d postgres -qc "create database $DB" >/dev/null
    URL="postgres://jobrun:jobrun@localhost:5434/$DB?sslmode=disable"
    DATABASE_URL=$URL LOG_FORMAT=text LOG_LEVEL=warn WORKER_QUEUES=default,bench "$S/bin/jobrund" >"$OUT/$NAME.jobrund.log" 2>&1 &
    PID=$!
    for i in $(seq 1 60); do "$S/bin/jobrun" health >/dev/null 2>&1 && break; sleep 0.5; done
    sleep 2
    FLAG=""; [ "$MODE" = direct ] && FLAG="--direct"
    {
      echo "# $(date -u +%Y-%m-%dT%H:%M:%SZ) round=$r cmd: jobrun bench --rate $RATE --duration $DUR $FLAG (fresh db $DB)"
      DATABASE_URL=$URL "$S/bin/jobrun" bench --rate "$RATE" --duration "$DUR" $FLAG 2>&1
      echo "# exit=$?"
    } >"$OUT/$NAME.txt"
    kill $PID 2>/dev/null; wait $PID 2>/dev/null
    sleep 2
    docker exec jobrun-postgres-1 psql -U jobrun -d postgres -qc "drop database if exists $DB with (force)" >/dev/null
    echo "done $NAME: $(grep -E '^(enqueue|verdict|latency|completed|PASS|FAIL)' "$OUT/$NAME.txt" | tr '\n' ' ')"
  done
done
echo ALL_DONE
