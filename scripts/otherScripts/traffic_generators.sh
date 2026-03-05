#!/usr/bin/env bash
set -e

SINK=${SINK_IP:-10.10.0.3}

benign() {
  sudo ip netns exec nsA wrk -t4 -c40 -d20s http://$SINK:18082/
}

scan() {
  sudo ip netns exec nsC nping --tcp -p 18080-18085 --rate 20 --count 200 $SINK
}

c2() {
  sudo ip netns exec nsC bash -c '
  for i in $(seq 1 40); do
     curl -m 0.3 -s http://'$SINK':18083/ >/dev/null
     sleep 1
  done'
}

lateral() {
  sudo ip netns exec nsC hping3 -S -p 18084 -c 200 $SINK
}

exfil_burst() {
  sudo ip netns exec nsC wrk -t4 -c80 -d15s http://$SINK:18080/
}

exfil_lowslow() {
  sudo ip netns exec nsC bash -c '
  for i in $(seq 1 120); do
     curl -m 0.2 -s http://'$SINK':18081/ >/dev/null
     sleep 0.5
  done'
}
