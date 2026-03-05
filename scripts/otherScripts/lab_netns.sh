#!/usr/bin/env bash
set -euo pipefail

BR=br-lab
NS_A=nsA
NS_B=nsB
NS_C=nsC

IP_A=10.10.0.1/24
IP_B=10.10.0.2/24
IP_C=10.10.0.3/24

up() {
  sudo ip link add "$BR" type bridge 2>/dev/null || true
  sudo ip link set "$BR" up

  for ns in "$NS_A" "$NS_B" "$NS_C"; do
    sudo ip netns add "$ns" 2>/dev/null || true
    sudo ip -n "$ns" link set lo up
  done

  # veth pairs
  sudo ip link add vethA type veth peer name vethA-br 2>/dev/null || true
  sudo ip link add vethB type veth peer name vethB-br 2>/dev/null || true
  sudo ip link add vethC type veth peer name vethC-br 2>/dev/null || true

  sudo ip link set vethA netns "$NS_A"
  sudo ip link set vethB netns "$NS_B"
  sudo ip link set vethC netns "$NS_C"

  sudo ip link set vethA-br master "$BR"
  sudo ip link set vethB-br master "$BR"
  sudo ip link set vethC-br master "$BR"

  sudo ip link set vethA-br up
  sudo ip link set vethB-br up
  sudo ip link set vethC-br up

  sudo ip -n "$NS_A" addr add "$IP_A" dev vethA
  sudo ip -n "$NS_B" addr add "$IP_B" dev vethB
  sudo ip -n "$NS_C" addr add "$IP_C" dev vethC

  sudo ip -n "$NS_A" link set vethA up
  sudo ip -n "$NS_B" link set vethB up
  sudo ip -n "$NS_C" link set vethC up

  echo "[OK] netns up"
  echo "A: $NS_A vethA $IP_A"
  echo "B: $NS_B vethB $IP_B"
  echo "C: $NS_C vethC $IP_C"
}

down() {
  set +e
  sudo ip netns del "$NS_A" 2>/dev/null
  sudo ip netns del "$NS_B" 2>/dev/null
  sudo ip netns del "$NS_C" 2>/dev/null
  sudo ip link del "$BR" 2>/dev/null
  echo "[OK] netns down"
}

case "${1:-}" in
  up) up ;;
  down) down ;;
  *) echo "Usage: $0 up|down" ; exit 1 ;;
esac
