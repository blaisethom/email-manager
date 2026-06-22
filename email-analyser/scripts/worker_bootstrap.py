#!/usr/bin/env python3
"""Install email-manager for the Prefect worker.

ensurepip has pip bundled but can only install it into the Python
environment's site-packages, which is read-only for the worker process.
Instead we use the bundled pip wheel directly with --target to install
into a user-writable directory exposed via PYTHONPATH.
"""
import os
import subprocess
import sys

HOME = os.path.expanduser("~")
DEPS = os.path.join(HOME, ".email-manager-deps")
# This script lives at email-analyser/scripts/worker_bootstrap.py
SRC = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

os.makedirs(DEPS, exist_ok=True)

# Locate ensurepip's bundled pip wheel — it ships with every CPython 3.4+
import ensurepip
bundled = os.path.join(os.path.dirname(ensurepip.__file__), "_bundled")
pip_whl = next(f for f in os.listdir(bundled) if f.startswith("pip-"))
pip_whl_path = os.path.join(bundled, pip_whl)

# Augment PYTHONPATH so the subprocess python can import pip from the wheel
env = {**os.environ, "PYTHONPATH": pip_whl_path + os.pathsep + os.environ.get("PYTHONPATH", "")}

def run_pip(*args):
    subprocess.check_call([sys.executable, "-W", "ignore", "-m", "pip", *args], env=env)

# Install all dependencies (psycopg2-binary, anthropic, etc.) — skip if present
run_pip("install", SRC + "[scheduler,postgres]", "--target", DEPS, "--quiet")

# Force-reinstall our package only to pick up code changes without re-downloading deps
run_pip("install", SRC, "--target", DEPS, "--force-reinstall", "--no-deps", "--quiet")

print(f"email-manager installed to {DEPS}")
