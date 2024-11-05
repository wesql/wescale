#!/bin/bash

PROMETHEUS_CONTAINER="prometheus"
GRAFANA_CONTAINER="grafana"

NETWORK_NAME="monitoring"

echo "Stopping and removing Prometheus container..."
docker stop $PROMETHEUS_CONTAINER 2>/dev/null
docker rm $PROMETHEUS_CONTAINER 2>/dev/null

echo "Stopping and removing Grafana container..."
docker stop $GRAFANA_CONTAINER 2>/dev/null
docker rm $GRAFANA_CONTAINER 2>/dev/null

echo "Removing Docker network '$NETWORK_NAME'..."
docker network rm $NETWORK_NAME 2>/dev/null

echo "Cleanup completed."