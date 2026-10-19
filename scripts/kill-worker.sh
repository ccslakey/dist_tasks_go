#!/bin/sh
# Kills (or with -pause N, freezes for N seconds) one random worker container
# from the docker compose demo, then brings the fleet back to full size.
set -eu
cd "$(dirname "$0")/.."

victim=$(docker compose ps -q worker | awk 'BEGIN{srand()} {a[NR]=$0} END{if (NR) print a[int(rand()*NR)+1]}')
[ -n "$victim" ] || { echo "no running workers; start the demo with: docker compose up -d --build" >&2; exit 1; }
name=$(docker inspect -f '{{.Name}}' "$victim" | sed 's#^/##')

if [ "${1:-}" = "-pause" ]; then
	secs=${2:-10}
	echo "freezing $name for ${secs}s (its leases expire after 5s, so it comes back as a zombie)"
	docker pause "$victim" >/dev/null
	sleep "$secs"
	docker unpause "$victim" >/dev/null
	echo "resumed $name"
else
	echo "SIGKILL $name"
	docker kill --signal KILL "$victim" >/dev/null
	sleep 3
	docker compose up -d --no-recreate worker >/dev/null 2>&1
	echo "restarted the worker fleet"
fi
