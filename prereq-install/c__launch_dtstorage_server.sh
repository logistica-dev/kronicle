#!/bin/sh

cd src
uvicorn kronicle.main:app --reload --host 0.0.0.0 --port 8000 --log-level debug --access-log
