#!/usr/bin/env python3
"""Bootstrap pip (if missing) then install email-manager for the Prefect worker.

Runs as part of the Prefect deployment pull step before each flow run.
Uses sys.executable so it always targets the same Python the worker uses.
"""
import importlib
import os
import subprocess
import sys
import tempfile
import urllib.request


def _have_pip() -> bool:
    try:
        importlib.import_module("pip")
        return True
    except ImportError:
        return False


def _bootstrap_pip() -> None:
    # Try the standard-library ensurepip first (works on most CPython builds)
    r = subprocess.run([sys.executable, "-m", "ensurepip"], capture_output=True)
    if r.returncode == 0 and _have_pip():
        print("pip bootstrapped via ensurepip")
        return

    # Fall back to the official get-pip.py (requires outbound HTTPS)
    print("ensurepip unavailable or failed — fetching get-pip.py")
    fd, fname = tempfile.mkstemp(suffix=".py")
    os.close(fd)
    try:
        urllib.request.urlretrieve("https://bootstrap.pypa.io/get-pip.py", fname)
        subprocess.check_call([sys.executable, fname, "--quiet"])
        print("pip bootstrapped via get-pip.py")
    finally:
        os.unlink(fname)


if not _have_pip():
    _bootstrap_pip()

if not _have_pip():
    print("ERROR: could not install pip", file=sys.stderr)
    sys.exit(1)

# Install the package (editable so imports resolve from the cloned source tree)
subprocess.check_call([
    sys.executable, "-m", "pip", "install",
    "-e", ".[scheduler,postgres]",
    "--quiet",
])
print(f"email-manager installed into {sys.executable}")
