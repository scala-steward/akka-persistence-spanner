#!/bin/bash -x

echo "Setting up emulator"

docker-compose version && docker-compose  -f docker/docker-compose.yml up -d

gcloud config configurations create emulator || true # may already exist
gcloud config set api_endpoint_overrides/spanner http://localhost:9020/
gcloud config set project akka
gcloud spanner instances create akka --config=emulator-config --description="Test Instance" --nodes=1 || true # may already exist
