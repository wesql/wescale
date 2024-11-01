docker network create monitoring

docker run -d \
  --name=prometheus \
  --network=monitoring \
  -p 9090:9090 \
  -v ./prometheus.yml:/etc/prometheus/prometheus.yml \
  -v ./prometheus_rules.yaml:/etc/prometheus/prometheus_rules.yaml \
  prom/prometheus


# datasource: http://prometheus:9090
docker run -d \
  --name=grafana \
  --network=monitoring \
  -p 3000:3000 \
  grafana/grafana