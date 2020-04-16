#!/bin/bash -x
docker-compose version && docker-compose  -f docker/docker-compose.yml up -d
