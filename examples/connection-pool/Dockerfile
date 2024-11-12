# Build Stage
FROM golang:1.23.3-bullseye AS builder

WORKDIR /app

COPY . .

# Disable CGO and build a statically linked binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/connection-pool-tester

# Final Stage
FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/bin/connection-pool-tester .

# Ensure the binary has execute permissions
RUN chmod +x ./connection-pool-tester

ENV MYSQL_HOST="localhost" \
    MYSQL_PORT=3306 \
    MYSQL_USER="root" \
    MYSQL_PASSWORD="passwd" \
    MYSQL_DATABASE="information_schema" \
    CONNECTION_COUNT="100" \
    DURATION="5m" \
    QUERY="SELECT 1" \
    EXIT_ON_FAILURE="true"

# Use shell form for ENTRYPOINT to enable variable substitution
ENTRYPOINT ./connection-pool-tester \
    --mysql-host=${MYSQL_HOST} \
    --mysql-port=${MYSQL_PORT} \
    --mysql-user=${MYSQL_USER} \
    --mysql-password=${MYSQL_PASSWORD} \
    --mysql-database=${MYSQL_DATABASE} \
    --connection-count=${CONNECTION_COUNT} \
    --duration=${DURATION} \
    --query="${QUERY}" \
    --exit-on-failure=${EXIT_ON_FAILURE}