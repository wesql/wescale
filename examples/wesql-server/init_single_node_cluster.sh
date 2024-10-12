#!/bin/bash
set -m

if [ "$(id -u)" -eq 0 ]; then
  exec su -s /bin/bash vitess "$0" "$@"
fi

# Set default values
export MYSQL_ROOT_USER=${MYSQL_ROOT_USER:-'root'}
export MYSQL_ROOT_PASSWORD=${MYSQL_ROOT_PASSWORD:-'passwd'}
export MYSQL_HOST=${MYSQL_HOST:-'127.0.0.1'}
export MYSQL_PORT=${MYSQL_PORT:-'3306'}

export VTTABLET_ID="${VTTABLET_ID:-0}"
export VTTABLET_PORT=${VTTABLET_PORT:-'15100'}
export VTTABLET_GRPC_PORT=${VTTABLET_GRPC_PORT:-'16100'}

export VTGATE_WEB_PORT=${VTGATE_WEB_PORT:-'15001'}
export VTGATE_GRPC_PORT=${VTGATE_GRPC_PORT:-'15991'}
export VTGATE_MYSQL_PORT=${VTGATE_MYSQL_PORT:-'15306'}

export VTDATAROOT=${VTDATAROOT:-$(pwd)/vtdataroot}

# Define a function to catch script exit signals and clean up background processes
cleanup() {
  echo "Cleaning up background processes..."
  pkill -f etcd
  pkill -f vtctld
  pkill -f vttablet
  pkill -f vtgate
  exit 0
}

# Catch exit signals
trap cleanup SIGINT SIGTERM EXIT

# Define a function to add prefix to output
run_with_prefix() {
  prefix=$1
  shift
  "$@" 2>&1 | sed "s/^/[$prefix] /" &
}

# Function to wait for MySQL to become available using mysql command
wait_for_mysql() {
  echo "Checking MySQL connection..."
  local MAX_WAIT_TIME=600  # Maximum wait time in seconds (5 minutes)
  local WAIT_INTERVAL=5    # Interval between connection attempts in seconds
  local ELAPSED_TIME=0

  while true; do
    if mysql -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_ROOT_USER" -p"$MYSQL_ROOT_PASSWORD" -e "SELECT 1;" >/dev/null 2>&1; then
      echo "MySQL is available."
      break
    else
      echo "MySQL is not available yet. Waiting..."
      sleep $WAIT_INTERVAL
      ELAPSED_TIME=$((ELAPSED_TIME + WAIT_INTERVAL))
      if [ $ELAPSED_TIME -ge $MAX_WAIT_TIME ]; then
        echo "Error: Unable to connect to MySQL after $MAX_WAIT_TIME seconds."
        exit 1
      fi
    fi
  done
}

# Wait for MySQL to be available
./wait-for-service.sh mysql $MYSQL_HOST $MYSQL_PORT

echo "Initializing single-node cluster..."

# Start etcd
echo "Starting etcd..."
run_with_prefix "etcd" ./etcd.sh
echo "Waiting for etcd service to start..."
./wait-for-service.sh etcd 127.0.0.1 2379
echo "etcd has started."

# etcd post-start configuration
echo "Executing etcd post-start configuration..."
./etcd-post-start.sh

# Start vtctld
echo "Starting vtctld..."
run_with_prefix "vtctld" ./vtctld.sh
echo "Waiting for vtctld service to start..."
./wait-for-service.sh vtctld 127.0.0.1 15999
echo "vtctld has started."

# Start vttablet
echo "Starting vttablet..."
run_with_prefix "vttablet" ./vttablet.sh
echo "Waiting for vttablet service to start..."
./wait-for-service.sh vttablet 127.0.0.1 $VTTABLET_GRPC_PORT
echo "vttablet has started."

# Start vtgate
echo "Starting vtgate..."
run_with_prefix "vtgate" ./vtgate.sh
echo "Waiting for vtgate service to start..."
./wait-for-service.sh vtgate 127.0.0.1 $VTGATE_MYSQL_PORT
echo "vtgate has started."

echo "
------------------------------------------------------------------------
"

echo "VTGate endpoint:
mysql -h127.0.0.1 -P$VTGATE_MYSQL_PORT
"

# Keep the script running to catch exit signals
wait