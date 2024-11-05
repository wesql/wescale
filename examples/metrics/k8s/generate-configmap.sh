kubectl create configmap wesql-prometheus-config \
  --from-file=../prometheus.yml \
  --from-file=../prometheus_rules.yaml \
  -n monitoring \
  --dry-run=client -o yaml > wesql-prometheus-configmap.yaml

kubectl create configmap wesql-grafana-config \
  --from-file=../prometheus-datasource.yaml \
  --from-file=../dashboards.yaml \
  --from-file=../performance_overview.json \
  -n monitoring \
  --dry-run=client -o yaml > wesql-grafana-configmap.yaml