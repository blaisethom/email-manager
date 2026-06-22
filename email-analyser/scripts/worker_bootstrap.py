#!/usr/bin/env python3
"""Bootstrap pip (if missing) then install email-manager for the Prefect worker.

Runs as part of the Prefect deployment pull step before each flow run.
Uses sys.executable so it always targets the same Python the worker uses.
"""
import subprocess
import sys


def _pip_works() -> bool:
    r = subprocess.run([sys.executable, "-m", "pip", "--version"], capture_output=True)
    return r.returncode == 0


if not _pip_works():
    print("pip not available — trying ensurepip")
    r = subprocess.run([sys.executable, "-m", "ensurepip", "--upgrade"], capture_output=True)
    if r.returncode != 0:
        print(r.stderr.decode(), file=sys.stderr)
    if not _pip_works():
        print("ERROR: pip unavailable and ensurepip failed to install it", file=sys.stderr)
        sys.exit(1)
    print("pip bootstrapped via ensurepip")

# Install the package (editable so imports resolve from the cloned source tree)
subprocess.check_call([
    sys.executable, "-m", "pip", "install",
    "-e", ".[scheduler,postgres]",
    "--quiet",
])
print(f"email-manager installed into {sys.executable}")
