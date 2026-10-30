#!/usr/bin/env bash
# Drives the real PostgreSQL and MySQL command-line clients against a surreal server that serves
# both wire listeners, once without TLS and once with it.
#
#   ./run.sh [pg|my|all]
#
# Environment:
#   SURREAL_BIN   server binary (default: target/debug/surreal)
#   CI            when set, missing client tools are an error instead of a skip
#   SQL_CLIENTS_BASE_PORT  first of six consecutive ports (default 41000)
#
# The suite starts two in-memory servers, defines a read-only database user on each, runs the
# cases in pg.sh and my.sh, and stops the servers on exit. Cases marked `gap` are known
# client-visible gaps: they must keep failing, and the run fails when one starts passing.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../.." && pwd)"
SURREAL_BIN="${SURREAL_BIN:-$repo/target/debug/surreal}"
base="${SQL_CLIENTS_BASE_PORT:-41000}"
WHICH="${1:-all}"
case "$WHICH" in
all | pg | my) ;;
*)
	echo "usage: $0 [pg|my|all]" >&2
	exit 2
	;;
esac

HTTP_PORT=$base
PG_PORT=$((base + 1))
MY_PORT=$((base + 2))
HTTP_TLS_PORT=$((base + 3))
PG_TLS_PORT=$((base + 4))
MY_TLS_PORT=$((base + 5))

WORKDIR="$(mktemp -d)"
CA_FILE="$WORKDIR/ca.pem"
KEY_FILE="$WORKDIR/key.pem"
pids=()

stop_servers() {
	local pid
	for pid in ${pids[@]+"${pids[@]}"}; do
		kill -TERM "$pid" 2>/dev/null || true
	done
	for pid in ${pids[@]+"${pids[@]}"}; do
		wait "$pid" 2>/dev/null || true
	done
	rm -rf "$WORKDIR"
}
trap stop_servers EXIT
trap 'exit 130' INT TERM

# shellcheck source=lib.sh
source "$here/lib.sh"

port_in_use() { (echo >/dev/tcp/127.0.0.1/"$1") >/dev/null 2>&1; }

for port in $HTTP_PORT $PG_PORT $MY_PORT $HTTP_TLS_PORT $PG_TLS_PORT $MY_TLS_PORT; do
	if port_in_use "$port"; then
		echo "port $port is in use; set SQL_CLIENTS_BASE_PORT to a free range" >&2
		exit 2
	fi
done

[ -x "$SURREAL_BIN" ] || { echo "server binary not found: $SURREAL_BIN" >&2; exit 2; }

openssl req -x509 -newkey rsa:2048 -nodes -keyout "$KEY_FILE" -out "$CA_FILE" -days 2 \
	-subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"

start_server() {
	local name="$1" http="$2" pg="$3" my="$4"
	shift 4
	"$SURREAL_BIN" start --no-banner --user root --pass root \
		--bind "127.0.0.1:$http" --postgres-bind "127.0.0.1:$pg" --mysql-bind "127.0.0.1:$my" \
		"$@" memory >"$WORKDIR/$name.log" 2>&1 &
	pids+=("$!")
	for _ in $(seq 1 240); do
		if ! kill -0 "$!" 2>/dev/null; then
			echo "server $name exited early:" >&2
			cat "$WORKDIR/$name.log" >&2
			exit 3
		fi
		if curl -fs "http://127.0.0.1:$http/health" >/dev/null 2>&1 \
			|| curl -fsk "https://127.0.0.1:$http/health" >/dev/null 2>&1; then
			return 0
		fi
		sleep 0.5
	done
	echo "server $name did not become ready" >&2
	cat "$WORKDIR/$name.log" >&2
	exit 3
}

# A database-scoped viewer, for read-only authorization cases. Defined over HTTP because the
# SQL front ends do not speak SurrealQL's DEFINE USER.
define_viewer() {
	local scheme="$1" port="$2"
	curl -fsk -u root:root -H "surreal-ns: t" -H "surreal-db: t" -H "Accept: application/json" \
		-X POST "$scheme://127.0.0.1:$port/sql" \
		-d "DEFINE USER alice ON DATABASE PASSWORD 'secret' ROLES VIEWER;" >/dev/null
}

start_server plain "$HTTP_PORT" "$PG_PORT" "$MY_PORT"
start_server tls "$HTTP_TLS_PORT" "$PG_TLS_PORT" "$MY_TLS_PORT" --web-crt "$CA_FILE" --web-key "$KEY_FILE"
define_viewer http "$HTTP_PORT"
define_viewer https "$HTTP_TLS_PORT"

export HTTP_PORT PG_PORT MY_PORT HTTP_TLS_PORT PG_TLS_PORT MY_TLS_PORT CA_FILE WORKDIR

case "$WHICH" in
all | pg) source "$here/pg.sh" ;;
esac
case "$WHICH" in
all | my) source "$here/my.sh" ;;
esac

summary
