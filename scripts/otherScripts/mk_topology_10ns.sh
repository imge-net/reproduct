#!/usr/bin/env bash
set -euo pipefail

# Creates nsA..nsJ with veth pairs attached to br0, assigns IPv4 underlay,
# starts mycelium in each namespace (assumes myceliumd-private installed).

N="${1:-10}"                 # 10 => nsA..nsJ
BR="${BR:-br0}"
SUBNET="${SUBNET:-10.10.0.0/24}"
GW="${GW:-10.10.0.1}"

# map index->letter
letters=(A B C D E F G H I J K L M N O P)

need() { command -v "$1" >/dev/null 2>&1 || { echo "[ERR] missing $1"; exit 1; }; }
need ip
need bridge
need mycelium || true

echo "[INFO] creating bridge $BR (if missing)"
sudo ip link add name "$BR" type bridge 2>/dev/null || true
sudo ip link set "$BR" up

# clean old namespaces
for i in $(seq 0 $((N-1))); do
  ns="ns${letters[$i]}"
  sudo ip netns del "$ns" 2>/dev/null || true
done

# create namespaces + veth
for i in $(seq 0 $((N-1))); do
  ns="ns${letters[$i]}"
  veth_ns="veth${letters[$i]}"
  veth_br="veth${letters[$i]}b"
  ip4="10.10.0.$((i+2))"

  echo "[INFO] setup $ns ($ip4)"
  sudo ip netns add "$ns"
  sudo ip link add "$veth_ns" type veth peer name "$veth_br"
  sudo ip link set "$veth_ns" netns "$ns"

  sudo ip link set "$veth_br" master "$BR"
  sudo ip link set "$veth_br" up

  sudo ip netns exec "$ns" ip link set lo up
  sudo ip netns exec "$ns" ip link set "$veth_ns" up
  sudo ip netns exec "$ns" ip addr add "$ip4/24" dev "$veth_ns"
  sudo ip netns exec "$ns" ip route add default via "$GW" 2>/dev/null || true
done

# put GW IP on bridge (optional)
sudo ip addr add "$GW/24" dev "$BR" 2>/dev/null || true

echo "[INFO] starting mycelium in each namespace (background)"
for i in $(seq 0 $((N-1))); do
  ns="ns${letters[$i]}"
  # you may have your own mycelium start command; keep it minimal here
  sudo ip netns exec "$ns" bash -lc "myceliumd-private --version >/dev/null 2>&1 || true" || true
  sudo ip netns exec "$ns" bash -lc "nohup myceliumd-private > /tmp/${ns}_mycelium.log 2>&1 &" || true
done

echo "[OK] done. namespaces:"
sudo ip netns list | head
