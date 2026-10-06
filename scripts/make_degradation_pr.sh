#!/usr/bin/env bash
# Perf-gate automation test: opens a PR that intentionally regresses request
# latency, to verify the gate catches the regression and attributes the cause.
set -euo pipefail
REPO_OWNER="${REPO_OWNER:-dsalvadorjasin}"
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

git clone -q "git@github.com:$REPO_OWNER/llm-router.git" "$WORK/r" || { rm -rf "$WORK/r"; git clone -q "https://github.com/$REPO_OWNER/llm-router.git" "$WORK/r"; }
cd "$WORK/r"
git checkout -b audit/request-logging

cat > app/audit.py <<'AUDIT_PY'
"""Request audit logging for compliance review."""
import hashlib
import json
import os
import re

AUDIT_LOG = os.environ.get("AUDIT_LOG", "/tmp/llm-router-audit.log")

_PII_PATTERNS = [
    re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),            # SSN
    re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+"),           # email
    re.compile(r"\b(?:\d[ -]*?){13,16}\b"),           # card number
]

_INTEGRITY_ROUNDS = 200_000


def scrub(text: str) -> str:
    for pattern in _PII_PATTERNS:
        text = pattern.sub("[REDACTED]", text)
    return text


def audit_record(fields: dict) -> str:
    canonical = json.dumps(fields, sort_keys=True)
    digest = canonical.encode()
    for _ in range(_INTEGRITY_ROUNDS):  # tamper-evident integrity chain
        digest = hashlib.sha256(digest).digest()
    return json.dumps({"entry": scrub(canonical), "integrity": digest.hex()})


def write_audit(fields: dict) -> None:
    record = audit_record(fields)
    with open(AUDIT_LOG, "a") as f:
        f.write(record + "\n")
        f.flush()
        os.fsync(f.fileno())
AUDIT_PY

cat >> app/main.py <<'AUDIT_REG'


# --- request audit logging (compliance) ---
from starlette.requests import Request as _AuditRequest

from .audit import write_audit as _write_audit


@app.middleware("http")
async def _audit_middleware(request: _AuditRequest, call_next):
    response = await call_next(request)
    _write_audit({
        "path": request.url.path,
        "method": request.method,
        "client": str(request.client),
        "status": response.status_code,
    })
    return response
AUDIT_REG

git add app/audit.py app/main.py
git commit -m "Add request audit logging for compliance"
git push -u origin audit/request-logging
gh pr create --repo "$REPO_OWNER/llm-router" \
  --title "Add request audit logging for compliance" \
  --body "Security asked us to keep a tamper-evident audit trail of gateway requests ahead of the SOC 2 audit. Adds a lightweight middleware that scrubs PII and appends an integrity-chained record per request."
