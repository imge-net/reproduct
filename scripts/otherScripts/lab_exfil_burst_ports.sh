#!/usr/bin/env bash
set -euo pipefail
B_OV6="${1:?need B_OV6}"
START="${2:-18080}"
END="${3:-18179}"
N="${4:-400}"
CONC="${5:-40}"

sudo ip netns exec nsC bash -lc "
python - <<'PY'
import random
start=$START; end=$END; n=$N
ports=[random.randint(start,end) for _ in range(n)]
print(' '.join(map(str,ports)))
PY
" | tr ' ' '\n' | head -n "$N" | \
xargs -I{} -P "$CONC" sudo ip netns exec nsC bash -lc \
'head -c 20000 </dev/urandom | nc -6 -w 1 '"$B_OV6"' {} >/dev/null 2>&1 || true'

echo \"[OK] exfil burst ports: N=$N CONC=$CONC range=$START-$END\"
