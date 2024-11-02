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
  -v ./prometheus-datasource.yaml:/etc/grafana/provisioning/datasources/prometheus-datasource.yaml \
  -e "GF_AUTH_ANONYMOUS_ENABLED=true" \
  -e "GF_AUTH_ANONYMOUS_ORG_ROLE=Admin" \
  -e "GF_AUTH_DISABLE_LOGIN_FORM=true" \
  grafana/grafana