#!/usr/bin/env bash
set -euo pipefail

echo "[INFO] killing background runners..."
pkill -f run_four_phase_multi_vethB_bg_v2.sh 2>/dev/null || true
pkill -f run_four_phase_multi_vethB_bg.sh 2>/dev/null || true
pkill -f run_four_phase_multi 2>/dev/null || true
pkill -f lab_capture_four_phase_safe.sh 2>/dev/null || true
pkill -f lab_capture_three_phase_safe.sh 2>/dev/null || true
pkill -f run_mycelium_scale_portserver.sh 2>/dev/null || true
pkill -f portserver 2>/dev/null || true

echo "[INFO] stopping stray tcpdump in nsB (if any)..."
sudo ip netns exec nsB pkill -f tcpdump 2>/dev/null || true

echo "[OK] done"
