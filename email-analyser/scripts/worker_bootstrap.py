#!/usr/bin/env python3
"""Install email-manager for the Prefect worker.

Tries multiple pip locations since /opt/prefect has no pip and the
internet is restricted on this server.
"""
import os
import subprocess
import sys

HOME = os.path.expanduser("~")
DEPS = os.path.join(HOME, ".email-manager-deps")
SRC = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

os.makedirs(DEPS, exist_ok=True)

# Try pip commands in priority order (prefer Prefect's own pip if present)
PIP_CANDIDATES = [
    ["/opt/prefect/bin/pip"],
    ["/opt/prefect/bin/pip3"],
    ["/usr/bin/pip3"],
    ["/usr/bin/pip"],
    ["/usr/local/bin/pip3"],
    ["/usr/local/bin/pip"],
    ["/usr/bin/python3", "-m", "pip"],
    ["/usr/bin/python3.11", "-m", "pip"],
]

pip_cmd = None
for candidate in PIP_CANDIDATES:
    try:
        r = subprocess.run([*candidate, "--version"], capture_output=True, text=True)
        if r.returncode == 0:
            pip_cmd = candidate
            print(f"Found pip: {' '.join(candidate)} → {r.stdout.strip()}")
            break
    except (FileNotFoundError, OSError):
        continue

if pip_cmd is None:
    for d in ["/opt/prefect/bin/", "/usr/bin/", "/usr/local/bin/"]:
        if os.path.isdir(d):
            hits = [f for f in os.listdir(d) if "pip" in f.lower() or f.startswith("python")]
            if hits:
                print(f"  {d}: {hits}", file=sys.stderr)
    print("ERROR: no pip found on this system", file=sys.stderr)
    sys.exit(1)

# Install all dependencies (skip if already up-to-date)
subprocess.check_call([*pip_cmd, "install", f"{SRC}[scheduler,postgres]",
                       "--target", DEPS, "--quiet"])

# Force-reinstall our package only to pick up code changes
subprocess.check_call([*pip_cmd, "install", SRC,
                       "--target", DEPS, "--force-reinstall", "--no-deps", "--quiet"])

print(f"email-manager installed to {DEPS}")
