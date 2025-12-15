#!/bin/sh
echo "Starting CSV Loader..."

if [ -f /app/persisted_csv/processed.flag ]; then
    echo "Data already processed. Skipping import."
else
    python3 load_csv.py
    touch /app/persisted_csv/processed.flag
fi

echo "CSV Loader Completed."