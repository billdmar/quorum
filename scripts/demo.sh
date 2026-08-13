#!/usr/bin/env bash
# demo.sh — bring up a local 3-node quorum cluster over loopback TCP, write and
# read a key, then KILL THE LEADER and show the cluster re-elect and still serve
# the value. This is the  headline demo, runnable by anyone in one command:
#
#   ./scripts/demo.sh
#
# It builds the binary, launches three nodes, drives the scenario, prints what
# happened, and cleans up every process on exit (even on failure).
set -euo pipefail

cd "$(dirname "$0")/.."

PEERS="N1=127.0.0.1:9201,N2=127.0.0.1:9202,N3=127.0.0.1:9203"
CLIENTS="N1=127.0.0.1:8201,N2=127.0.0.1:8202,N3=127.0.0.1:8203"
# Node ids, their client ports, and (populated at launch) their pids — kept
# index-aligned so we can map an id back to the process to kill.
IDS=(N1 N2 N3)
CPORTS=(8201 8202 8203)
PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "== building quorum =="
go build -o bin/quorum ./cmd/quorum

echo "== launching 3-node cluster (loopback TCP) =="
for idx in "${!IDS[@]}"; do
  id="${IDS[$idx]}"; cport="${CPORTS[$idx]}"
  ./bin/quorum server -id "$id" -peers "$PEERS" -client "127.0.0.1:$cport" \
    >"/tmp/quorum-demo-$id.log" 2>&1 &
  PIDS+=("$!")
done

# Give the cluster a moment to elect a leader.
sleep 2

echo "== put foo=bar (via N1, following leader redirect) =="
./bin/quorum kv put -addr 127.0.0.1:8201 -peers "$CLIENTS" foo bar

echo "== get foo =="
got=$(./bin/quorum kv get -addr 127.0.0.1:8201 -peers "$CLIENTS" foo)
echo "  -> $got"

# find_leader: query each node's status endpoint and echo the index (into IDS/
# CPORTS/PIDS) of the node that reports itself leader, or nothing if none do yet.
find_leader() {
  for idx in "${!IDS[@]}"; do
    if [ "$(./bin/quorum kv status -addr 127.0.0.1:${CPORTS[$idx]} 2>/dev/null)" = "leader" ]; then
      echo "$idx"; return 0
    fi
  done
  return 1
}

echo "== find the ACTUAL leader =="
leader_idx=""
for _ in 1 2 3 4 5; do
  if leader_idx=$(find_leader); then break; fi
  sleep 1
done
if [ -z "$leader_idx" ]; then
  echo "== DEMO FAILED: no leader elected =="
  exit 1
fi
leader_id="${IDS[$leader_idx]}"
leader_pid="${PIDS[$leader_idx]}"
echo "  leader is $leader_id (pid $leader_pid)"

echo "== KILL the leader ($leader_id) and wait for re-election =="
kill "$leader_pid" 2>/dev/null || true
# Drop the killed node from all three index-aligned arrays.
unset 'IDS[leader_idx]' 'CPORTS[leader_idx]' 'PIDS[leader_idx]'
IDS=("${IDS[@]}"); CPORTS=("${CPORTS[@]}"); PIDS=("${PIDS[@]}")

# Confirm a NEW leader (re-election actually happened, not just survival).
new_leader_idx=""
for _ in 1 2 3 4 5; do
  if new_leader_idx=$(find_leader); then break; fi
  sleep 1
done
if [ -z "$new_leader_idx" ]; then
  echo "== DEMO FAILED: cluster did not re-elect a leader after the kill =="
  exit 1
fi
echo "  re-elected new leader: ${IDS[$new_leader_idx]}"

echo "== get foo again (from a surviving node) — value must survive the kill =="
survivor_port="${CPORTS[0]}"
got2=$(./bin/quorum kv get -addr "127.0.0.1:$survivor_port" -peers "$CLIENTS" foo)
echo "  -> $got2"

if [ "$got2" = "bar" ]; then
  echo "== DEMO OK: killed the leader ($leader_id), cluster re-elected ${IDS[$new_leader_idx]}, value survived =="
else
  echo "== DEMO FAILED: expected 'bar', got '$got2' =="
  exit 1
fi
