#!/usr/bin/env python3
"""Diagnostic: report which packages are already importable on the Prefect worker."""
import importlib
import os
import subprocess
import sys

PACKAGES = [
    "prefect", "pydantic", "pydantic_settings", "click", "rich",
    "httpx", "anyio", "anthropic", "dotenv",
    "imapclient", "bs4", "html2text", "yaml",
    "google.api_core", "google.auth", "google.oauth2",
    "googleapiclient", "psycopg2",
]

print(f"Python: {sys.executable} {sys.version}")
print(f"sys.path: {sys.path}")
print()

available, missing = [], []
for pkg in PACKAGES:
    try:
        importlib.import_module(pkg)
        available.append(pkg)
    except ImportError:
        missing.append(pkg)

print(f"Available ({len(available)}): {', '.join(available)}")
print(f"Missing  ({len(missing)}): {', '.join(missing)}")

# Also check site-packages directories
for path in sys.path:
    if "site-packages" in path and os.path.isdir(path):
        pkgs = sorted(os.listdir(path))[:30]
        print(f"\n{path} (first 30):\n  " + "\n  ".join(pkgs))
        break
