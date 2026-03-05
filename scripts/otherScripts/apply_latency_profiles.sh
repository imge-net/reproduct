#!/usr/bin/env bash
set -euo pipefail

# Apply different latency/jitter/loss profiles per namespace interface.
# Assumes underlay iface inside ns is vethX (e.g., vethA, vethB, ...)

PROFILE="${1:-A}"   # A/B/C
N="${2:-10}"

letters=(A B C D E F G H I J K L M N O P)

echo "[INFO] applying profile=$PROFILE to N=$N namespaces"

for i in $(seq 0 $((N-1))); do
  ns="ns${letters[$i]}"
  ifc="veth${letters[$i]}"

  # clear existing qdisc
  sudo ip netns exec "$ns" tc qdisc del dev "$ifc" root 2>/dev/null || true

  case "$PROFILE" in
    A) # low-latency
      sudo ip netns exec "$ns" tc qdisc add dev "$ifc" root netem delay 5ms 1ms ;;
    B) # medium + jitter
      sudo ip netns exec "$ns" tc qdisc add dev "$ifc" root netem delay 35ms 10ms ;;
    C) # harsh: delay + jitter + loss
      sudo ip netns exec "$ns" tc qdisc add dev "$ifc" root netem delay 80ms 20ms loss 1% ;;
    *)
      echo "[ERR] unknown profile $PROFILE (use A/B/C)"; exit 1 ;;
  esac
done

echo "[OK] applied profile=$PROFILE"
