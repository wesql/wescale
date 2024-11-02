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
  -e "GF_AUTH_ANONYMOUS_ENABLED=true" \
  -e "GF_AUTH_ANONYMOUS_ORG_ROLE=Admin" \
  -e "GF_AUTH_DISABLE_LOGIN_FORM=true" \
  -e "GF_DATASOURCES__Prometheus__Name=Prometheus" \
  -e "GF_DATASOURCES__Prometheus__Type=prometheus" \
  -e "GF_DATASOURCES__Prometheus__Access=proxy" \
  -e "GF_DATASOURCES__Prometheus__URL=http://prometheus:9090" \
  -e "GF_DATASOURCES__Prometheus__IsDefault=true" \
  grafana/grafana