#!/bin/bash
set -e

# Install Elasticsearch 9.x (Lucene 10)
wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch \
    | sudo gpg --dearmor --yes -o /usr/share/keyrings/elasticsearch-keyring.gpg
sudo apt-get install -y apt-transport-https
echo "deb [signed-by=/usr/share/keyrings/elasticsearch-keyring.gpg] https://artifacts.elastic.co/packages/9.x/apt stable main" \
    | sudo tee /etc/apt/sources.list.d/elastic-9.x.list
sudo apt-get update && sudo apt-get install -y elasticsearch=9.3.2

# Create data and log directories with correct ownership
sudo mkdir -p /data/elasticsearch /data/elasticsearch-logs
sudo chown -R elasticsearch:elasticsearch /data/elasticsearch /data/elasticsearch-logs

# Apply benchmark config
sudo cp config/elasticsearch.yml /etc/elasticsearch/elasticsearch.yml
sudo mkdir -p /etc/elasticsearch/jvm.options.d
sudo cp config/jvm.options /etc/elasticsearch/jvm.options.d/benchmark.options

# OS tuning
echo "vm.max_map_count=262144" | sudo tee -a /etc/sysctl.conf
sudo sysctl -w vm.max_map_count=262144

# Allow the elasticsearch process to lock memory (required for bootstrap.memory_lock)
sudo mkdir -p /etc/systemd/system/elasticsearch.service.d
sudo tee /etc/systemd/system/elasticsearch.service.d/override.conf << 'EOF'
[Service]
LimitMEMLOCK=infinity
EOF
sudo systemctl daemon-reload

# Python environment for ingest
# Apply persistent cluster settings (recovery speed)
sudo systemctl start elasticsearch
until curl -sf 'http://localhost:9200/_cluster/health?wait_for_status=yellow&timeout=5s' > /dev/null 2>&1; do sleep 1; done
curl -s -X PUT 'http://localhost:9200/_cluster/settings' \
    -H 'Content-Type: application/json' \
    -d '{"persistent":{"indices.recovery.max_bytes_per_sec":"500mb"}}'
sudo systemctl stop elasticsearch

sudo apt-get install -y python3-pip python3-venv
python3 -m venv "$(dirname "$0")/venv"
source "$(dirname "$0")/venv/bin/activate"
pip install --quiet pyarrow requests orjson
