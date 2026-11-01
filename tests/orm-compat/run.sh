#!/usr/bin/env bash
# One-command local runner for the ORM compatibility checks.
#
#   ./run.sh harness [--record]     targeted probes: SurrealDB, then the PostgreSQL control, then schema tools
#   ./run.sh upstream [ORM...]      upstream test suites (typeorm, drizzle, drizzle-kit, prisma; default all)
#   ./run.sh pr                     pull-request subset: the targeted probes on SurrealDB only, gated by expected-failures.json
#   ./run.sh services               start both servers and wait, for manual use (Ctrl-C stops them)
#
# ORM_COMPAT_SERVER=/path/to/surreal serves the checks from that server binary, started in memory
# without authentication, instead of the example listener built here.
#
# Ports 15432 (SurrealDB listener) and 15433 (PostgreSQL 18.4 reference) must be free. A second
# listener on a taken port fails silently, so the script refuses to start instead.
# Servers are always stopped on exit. `pr` runs in CI on pull requests, `upstream` nightly.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../.." && pwd)"
profile="${ORM_COMPAT_PROFILE:-release}"
profile_dir="$profile"
[ "$profile" = dev ] && profile_dir=debug
profile_flag=(--profile "$profile")
[ "$profile" = release ] && profile_flag=(--release)
[ "$profile" = dev ] && profile_flag=()
listener_bin="${CARGO_TARGET_DIR:-$repo/target}/$profile_dir/examples/orm_compat_server"
logs="${ORM_COMPAT_LOGS:-$repo/target/orm-compat-logs}"
mkdir -p "$logs"

pids=()

port_in_use() { (echo >/dev/tcp/127.0.0.1/"$1") >/dev/null 2>&1; }

require_free_ports() {
	local busy=0 port
	local ports=("$@")
	[ ${#ports[@]} -gt 0 ] || ports=(15432 15433)
	for port in "${ports[@]}"; do
		if port_in_use "$port"; then
			echo "port $port is already in use:" >&2
			lsof -nP -iTCP:"$port" -sTCP:LISTEN >&2 2>/dev/null || true
			busy=1
		fi
	done
	[ "$busy" = 0 ] || { echo "stop whatever holds those ports and retry" >&2; exit 2; }
}

stop_services() {
	local pid
	for pid in ${pids[@]+"${pids[@]}"}; do
		kill -TERM "$pid" 2>/dev/null || true
	done
	for pid in ${pids[@]+"${pids[@]}"}; do
		# The reference needs time to stop PostgreSQL itself; killing node first would orphan it.
		for _ in $(seq 1 150); do
			kill -0 "$pid" 2>/dev/null || break
			sleep 0.2
		done
		kill -KILL "$pid" 2>/dev/null || true
	done
	pids=()
}
trap stop_services EXIT
trap 'exit 130' INT TERM

wait_for_port() {
	local port="$1" name="$2" pid="$3" timeout="${4:-120}"
	for _ in $(seq 1 $((timeout * 2))); do
		if ! kill -0 "$pid" 2>/dev/null; then
			echo "$name exited before becoming ready; see $logs" >&2
			exit 3
		fi
		if (echo >/dev/tcp/127.0.0.1/"$port") >/dev/null 2>&1; then return 0; fi
		sleep 0.5
	done
	echo "$name did not open port $port within ${timeout}s" >&2
	exit 3
}

ensure_npm() {
	if [ ! -d "$here/node_modules" ]; then (cd "$here" && npm ci); fi
}

build_listener() {
	[ -z "${ORM_COMPAT_SERVER:-}" ] || return 0
	(cd "$repo" && cargo build --locked ${profile_flag[@]+"${profile_flag[@]}"} -p surrealdb-pgwire --example orm_compat_server)
}

listener_pid=""

start_listener() {
	if [ -n "${ORM_COMPAT_SERVER:-}" ]; then
		"$ORM_COMPAT_SERVER" start --log warn --unauthenticated --bind 127.0.0.1:0 \
			--postgres-bind 127.0.0.1:15432 memory >"$logs/listener.log" 2>&1 &
	else
		"$listener_bin" >"$logs/listener.log" 2>&1 &
	fi
	listener_pid="$!"
	pids+=("$listener_pid")
	wait_for_port 15432 "SurrealDB listener" "$listener_pid" 30
}

# The in-memory listener keeps every database it has served; a fresh one per ORM keeps runs independent.
restart_listener() {
	kill -TERM "$listener_pid" 2>/dev/null || true
	for _ in $(seq 1 50); do
		kill -0 "$listener_pid" 2>/dev/null || break
		sleep 0.2
	done
	kill -KILL "$listener_pid" 2>/dev/null || true
	start_listener
}

start_reference() {
	(cd "$here" && exec node reference.mjs) >"$logs/reference.log" 2>&1 &
	pids+=("$!")
	wait_for_port 15433 "PostgreSQL reference" "$!" 180
}

start_services() {
	require_free_ports
	ensure_npm
	build_listener
	start_listener
	start_reference
}

run_harness() {
	local record=0
	[ "${1:-}" = "--record" ] && record=1
	cd "$here"
	npm run generate
	local failed=0
	npm test || failed=1
	REFERENCE=1 PGPORT=15433 RESULT_PATH=reference-results.json npm test || failed=1
	npm run schema-suites || failed=1
	if [ "$record" = 1 ]; then
		if [ "$failed" = 0 ] || [ "${RECORD_ANYWAY:-0}" = 1 ]; then
			python3 record-results.py
		else
			echo "not recording: record-results.py requires every reference scenario to pass" >&2
		fi
	fi
	return "$failed"
}

# Pull-request subset: no PostgreSQL reference and no upstream suites. Every case must pass unless
# expected-failures.json lists it, and a listed case that passes fails the run.
run_pr() {
	cd "$here"
	rm -f results.json cli-results.json
	npm run generate
	local failed=0
	npm test || true
	ORM_COMPAT_TARGETS=surrealdb npm run schema-suites || true
	python3 pr-gate.py || failed=1
	return "$failed"
}

run_upstream() {
	cd "$here"
	local orms=("$@") status=0
	[ ${#orms[@]} -gt 0 ] || orms=(typeorm drizzle drizzle-kit prisma)
	local first=1
	for orm in "${orms[@]}"; do
		[ "$first" = 1 ] || restart_listener
		first=0
		python3 upstream/run.py --skip-classify --targets "${ORM_COMPAT_TARGETS:-reference,surrealdb}" "$orm" || status=1
	done
	python3 -c "import sys; sys.path.insert(0, 'upstream'); import classify; classify.main()" || status=1
	return "$status"
}

cmd="${1:-harness}"
[ $# -gt 0 ] && shift
case "$cmd" in
harness)
	start_services
	run_harness "$@"
	;;
pr)
	require_free_ports 15432
	ensure_npm
	build_listener
	start_listener
	run_pr
	;;
upstream)
	# The reference cluster grows to gigabytes over a full run; it is cheap to recreate, so it is removed on exit.
	[ "${ORM_COMPAT_KEEP_REFERENCE:-0}" = 1 ] || trap 'stop_services; rm -rf "$here/.postgres-reference"' EXIT
	start_services
	run_upstream "$@"
	;;
services)
	start_services
	echo "listener 127.0.0.1:15432 and reference 127.0.0.1:15433 are up; Ctrl-C to stop"
	while true; do sleep 3600; done
	;;
*)
	sed -n 2,9p "${BASH_SOURCE[0]}" >&2
	exit 64
	;;
esac
