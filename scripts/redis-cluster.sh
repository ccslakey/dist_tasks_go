#!/bin/sh
# Starts or stops a local Redis Cluster (3 primaries, no replicas, no
# persistence) for benchmarks and tests.
#
#   scripts/redis-cluster.sh start    # nodes on ports 30001-30003 (macOS AirPlay listens on 7000)
#   scripts/redis-cluster.sh status
#   scripts/redis-cluster.sh stop
#
#   go run ./cmd/bench -modes block -workers 8 -concurrency 256 -cluster 127.0.0.1:30001,127.0.0.1:30002,127.0.0.1:30003
#   TEST_REDIS_CLUSTER=127.0.0.1:30001,127.0.0.1:30002,127.0.0.1:30003 go test ./redisstream/ -run Sharded
#
# Override PORTS (space-separated), DIR, REDIS_SERVER, or REDIS_CLI.
set -eu
PORTS=${PORTS:-"30001 30002 30003"}
tmp=${TMPDIR:-/tmp}
DIR=${DIR:-${tmp%/}/dtq-redis-cluster}
SERVER=${REDIS_SERVER:-redis-server}
CLI=${REDIS_CLI:-redis-cli}

seeds() {
  out=""
  for port in $PORTS; do out="$out${out:+,}127.0.0.1:$port"; done
  echo "$out"
}

case "${1:-}" in
start)
  for port in $PORTS; do
    if "$CLI" -p "$port" ping >/dev/null 2>&1; then
      echo "something is already listening on $port; run '$0 stop' first" >&2
      exit 1
    fi
  done
  rm -rf "$DIR"
  for port in $PORTS; do
    mkdir -p "$DIR/$port"
    "$SERVER" --port "$port" --dir "$DIR/$port" \
      --cluster-enabled yes --cluster-config-file nodes.conf --cluster-node-timeout 5000 \
      --save "" --appendonly no \
      --daemonize yes --logfile "$DIR/$port/redis.log" --pidfile "$DIR/$port/redis.pid"
  done
  for port in $PORTS; do
    until "$CLI" -p "$port" ping >/dev/null 2>&1; do sleep 0.1; done
  done
  # shellcheck disable=SC2046 # one argument per node
  "$CLI" --cluster create $(seeds | tr ',' ' ') --cluster-replicas 0 --cluster-yes >"$DIR/create.log"
  for port in $PORTS; do
    until "$CLI" -p "$port" cluster info | grep -q cluster_state:ok; do sleep 0.1; done
  done
  echo "Redis Cluster up ($("$SERVER" --version | cut -d' ' -f3 | sed 's/^v=/Redis /')), data in $DIR"
  echo "seeds: $(seeds)"
  ;;
stop)
  for port in $PORTS; do
    "$CLI" -p "$port" shutdown nosave >/dev/null 2>&1 || true
  done
  rm -rf "$DIR"
  echo "Redis Cluster stopped"
  ;;
status)
  "$CLI" -p "${PORTS%% *}" cluster info | grep -E 'cluster_state|cluster_known_nodes|cluster_slots_assigned'
  "$CLI" -p "${PORTS%% *}" cluster nodes
  ;;
*)
  echo "usage: $0 start|stop|status" >&2
  exit 2
  ;;
esac
