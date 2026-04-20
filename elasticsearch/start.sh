#!/bin/bash
set -e

sudo systemctl daemon-reload
sudo systemctl enable elasticsearch
sudo systemctl start elasticsearch

echo "Waiting for Elasticsearch to start..."
for i in $(seq 1 60); do
    if curl -sf "http://localhost:9200/_cluster/health" > /dev/null 2>&1; then
        echo "Elasticsearch is up."
        exit 0
    fi
    echo "  attempt $i/60 — not ready yet, waiting 5s..."
    sleep 5
done

echo "ERROR: Elasticsearch did not start within 5 minutes." >&2
exit 1
