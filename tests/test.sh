#!/usr/bin/env bash
# Odyssey/Harbor verifier — writes /logs/verifier/reward.txt
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "${SCRIPT_DIR}/../task.toml" ]]; then
  APP_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
elif [[ -d /app ]]; then
  APP_ROOT="/app"
else
  APP_ROOT="$(pwd)"
fi

cd "$APP_ROOT"
export APP_ROOT PYTHONUNBUFFERED=1
export PYTHONPATH="${APP_ROOT}${PYTHONPATH:+:$PYTHONPATH}"

if [[ ! -d "${APP_ROOT}/tests" && -d /tests ]]; then
  ln -sfn /tests "${APP_ROOT}/tests"
fi

if mkdir -p /logs/verifier 2>/dev/null; then
  REWARD_DIR="/logs/verifier"
else
  REWARD_DIR="${APP_ROOT}/logs/verifier"
  mkdir -p "$REWARD_DIR"
fi
export REWARD_DIR
echo "0" > "${REWARD_DIR}/reward.txt"

python3 - <<'PY'
import json, os, sys, re, subprocess
from pathlib import Path
import pytest

ROOT = Path(os.environ["APP_ROOT"]).resolve()
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)
REWARD_DIR = Path(os.environ["REWARD_DIR"])
REWARD_DIR.mkdir(parents=True, exist_ok=True)

CHANNELS = {
    "visible_suite": {"weight": 0.10, "kind": "visible"},
    "monotonic_lsn": {
        "weight": 0.12,
        "args": ["tests/hidden/test_grading.py::test_h_monotonic_lsn_and_checksum"],
    },
    "checkpoint_pitr_lsn": {
        "weight": 0.14,
        "args": ["tests/hidden/test_grading.py::test_h_checkpoint_prefix_then_pitr_lsn"],
    },
    "pitr_time_delete": {
        "weight": 0.12,
        "args": ["tests/hidden/test_grading.py::test_h_pitr_by_time_and_delete"],
    },
    "restart": {
        "weight": 0.10,
        "args": ["tests/hidden/test_grading.py::test_h_restart_same_sqlite"],
    },
    "concurrent_lsn": {
        "weight": 0.14,
        "args": ["tests/hidden/test_grading.py::test_h_concurrent_unique_lsn"],
    },
    "ckpt_interrupt": {
        "weight": 0.08,
        "args": ["tests/hidden/test_grading.py::test_h_checkpoint_interrupt_atomic"],
    },
    "restore_interrupt": {
        "weight": 0.08,
        "args": ["tests/hidden/test_grading.py::test_h_restore_interrupt_atomic"],
    },
    "optimistic_locking": {
        "weight": 0.06,
        "args": ["tests/hidden/test_grading.py::test_h_optimistic_concurrency"],
    },
    "metrics_audit": {
        "weight": 0.06,
        "args": ["tests/hidden/test_grading.py::test_h_metrics_and_audit"],
    },
}

results = {}
total = 0.0
for name, cfg in CHANNELS.items():
    if cfg.get("kind") == "visible":
        r = subprocess.run(
            [sys.executable, "-m", "pytest", "-q", "tests/visible", "--maxfail=20"],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            env={**os.environ, "PYTHONPATH": str(ROOT)},
        )
        out = (r.stdout or "") + (r.stderr or "")
        m = re.search(r"(\d+) passed", out)
        skipped = re.search(r"(\d+) skipped", out)
        passed_n = int(m.group(1)) if m else 0
        skip_n = int(skipped.group(1)) if skipped else 0
        passed = r.returncode == 0 and passed_n >= 3 and skip_n <= passed_n
        detail = out[-400:]
        code = r.returncode
    else:
        code = pytest.main(["-q", "--tb=line", *cfg["args"]])
        passed = code == 0
        detail = ""
    score = cfg["weight"] if passed else 0.0
    results[name] = {
        "passed": passed,
        "weight": cfg["weight"],
        "score": score,
        "exit_code": int(code),
        "detail": detail,
    }
    total += score

reward = round(total, 4)
payload = {"channels": results, "score": reward, "max_score": 1.0, "reward": reward}
(REWARD_DIR / "report.json").write_text(json.dumps(payload, indent=2) + "\n")
(REWARD_DIR / "score.txt").write_text(f"{reward}\n")
value = f"{reward:.6f}".rstrip("0").rstrip(".") or "0"
(REWARD_DIR / "reward.txt").write_text(value + "\n")
(REWARD_DIR / "reward.json").write_text(json.dumps({"reward": float(reward)}) + "\n")
print(json.dumps(payload, indent=2))
print(f"REWARD={reward}")
sys.exit(0)
PY

echo "SCORE=$(cat "${REWARD_DIR}/reward.txt" 2>/dev/null || echo 0)"
[[ -f "${REWARD_DIR}/reward.txt" ]] || echo "0" > "${REWARD_DIR}/reward.txt"
exit 0
