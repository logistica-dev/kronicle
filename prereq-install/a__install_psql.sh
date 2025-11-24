#!/bin/sh

import os
import sys
import subprocess
import platform
import glob

def start_timescaledb():
    system = platform.system()

    if system == "Darwin":  # macOS
        print("Detected macOS")
        # Try to dynamically add postgres bin folder to PATH
        paths = glob.glob("/*/*/opt/postgresql@17/bin")
        if paths:
            os.environ["PATH"] = f"{paths[0]}:{os.environ['PATH']}"
            print(f"Added {paths[0]} to PATH")
        else:
            print("Warning: Could not find PostgreSQL bin folder. Installing postgresql@17 via brew.")
            brew install postgresql@17 timescaledb

        # Start PostgreSQL service via brew
        print("Starting PostgreSQL service (brew)...")
        subprocess.run(["brew", "services", "start", "postgresql@17"], check=True)

    elif system == "Linux":
        print("Detected Linux")
        # Assumes postgres/timescaledb is installed and systemd service exists
        subprocess.run(["sudo", "systemctl", "start", "postgresql"], check=True)
    else:
        raise RuntimeError(f"Unsupported OS: {system}")

    # Enable TimescaleDB extension in the default 'postgres' database
    print("Enabling TimescaleDB extension in 'postgres' database if not already enabled...")
    subprocess.run(["psql", "-d", "postgres", "-c", "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"], check=True)

    # Verify
    print("Verifying TimescaleDB installation...")
    result = subprocess.run(["psql", "-d", "postgres", "-c", r"\dx"], capture_output=True, text=True)
    print(result.stdout)

# Run the function
start_timescaledb()
