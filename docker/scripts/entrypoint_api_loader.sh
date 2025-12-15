#!/bin/sh
echo "Starting API Loader..."

if curl -X HEAD "http://elasticsearch:9200/some_index" -u "elastic:${ELASTIC_PASSWORD}" -I | grep -q "200 OK"; then
    echo "API data already indexed. Skipping import."
else
    python3 load_api.py
fi

echo "API Loader Completed."