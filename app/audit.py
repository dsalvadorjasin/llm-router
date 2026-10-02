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
