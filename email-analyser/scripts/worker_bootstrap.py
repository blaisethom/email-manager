#!/usr/bin/env python3
"""Install email-manager deps for the Prefect worker from vendored wheels.

The Prefect server has no pip and restricted internet access.
We bundle all required wheels in vendor/ and use the vendored pip.whl
to install them to ~/.email-manager-deps (exposed via PYTHONPATH).
The email_manager package itself is copied from the cloned source tree.
"""
import glob
import os
import shutil
import subprocess
import sys

HOME = os.path.expanduser("~")
DEPS = os.path.join(HOME, ".email-manager-deps")
# script is at email-analyser/scripts/worker_bootstrap.py
SRC = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VENDOR = os.path.join(SRC, "vendor")

os.makedirs(DEPS, exist_ok=True)

# Use the pip wheel bundled in vendor/ (no system pip required)
pip_whl = glob.glob(os.path.join(VENDOR, "pip-*.whl"))
if not pip_whl:
    print(f"ERROR: no pip wheel found in {VENDOR}", file=sys.stderr)
    sys.exit(1)
env = {**os.environ, "PYTHONPATH": pip_whl[0] + os.pathsep + os.environ.get("PYTHONPATH", "")}

def run_pip(*args):
    subprocess.check_call([sys.executable, "-W", "ignore", "-m", "pip", *args], env=env)

# Install all vendored wheels except pip itself
wheels = sorted(
    w for w in glob.glob(os.path.join(VENDOR, "*.whl"))
    if not os.path.basename(w).startswith("pip-")
)
print(f"Installing {len(wheels)} vendored packages to {DEPS}...")
run_pip("install", "--target", DEPS, "--no-deps", "--quiet", *wheels)

# Copy email_manager source from the cloned repo (always gets latest code)
em_src = os.path.join(SRC, "src", "email_manager")
em_dst = os.path.join(DEPS, "email_manager")
if os.path.exists(em_dst):
    shutil.rmtree(em_dst)
shutil.copytree(em_src, em_dst)

print(f"Done — email_manager + {len(wheels)} deps installed to {DEPS}")
