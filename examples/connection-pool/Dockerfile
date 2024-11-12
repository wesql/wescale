FROM golang:1.23.3-bullseye as builder

WORKDIR /app

COPY . .
RUN go build -o bin/connection-pool-tester

FROM alpine:latest

WORKDIR /app
COPY --from=builder /app/bin/connection-pool-tester .

ENV MYSQL_HOST="localhost" \
    MYSQL_PORT="3306" \
    MYSQL_USER="root" \
    MYSQL_PASSWORD="passwd" \
    MYSQL_DATABASE="information_schema" \
    CONNECTION_COUNT="100" \
    DURATION="5m" \
    QUERY="SELECT 1"

ENTRYPOINT ["./connection-pool-tester", \
    "--mysql-host=${MYSQL_HOST}", \
    "--mysql-port=${MYSQL_PORT}", \
    "--mysql-user=${MYSQL_USER}", \
    "--mysql-password=${MYSQL_PASSWORD}", \
    "--mysql-database=${MYSQL_DATABASE}", \
    "--connection-count=${CONNECTION_COUNT}", \
    "--duration=${DURATION}", \
    "--query=${QUERY}"]