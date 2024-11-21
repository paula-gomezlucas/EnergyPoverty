#!/bin/sh

echo "Starting API to Elasticsearch loader..."
python3 load_api.py
while true; do
    echo "Fetching latest data..."
    python3 load_api.py
    sleep 86400  # Run every day
done