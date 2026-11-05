#!/usr/bin/env bash
# Assertion helpers shared by the PostgreSQL and MySQL client suites. Sourced, never executed.
#
# A case runs one client command, captures stdout, stderr and the exit status, and compares them.
# `gap "reason"` marks the next case as a tracked known gap: it must fail, and it is reported as a
# gap; if it starts passing the suite fails so the marker gets removed.

PASSED=0
FAILED=0
GAPS=0
FAILED_NAMES=()
GAP_NAMES=()
PENDING_GAP=""
OUT=""
ERR=""
RC=0

gap() { PENDING_GAP="$*"; }

# Run a command and capture its output without letting `set -e` end the suite.
run() {
	local errfile
	errfile="$(mktemp)"
	set +e
	OUT="$("$@" 2>"$errfile")"
	RC=$?
	set -e
	ERR="$(cat "$errfile")"
	rm -f "$errfile"
}

record() {
	local name="$1" ok="$2" detail="$3"
	if [ -n "$PENDING_GAP" ]; then
		if [ "$ok" = 1 ]; then
			FAILED=$((FAILED + 1))
			FAILED_NAMES+=("$name (known gap now passes, drop the gap marker)")
			echo "FAIL $name: expected the known gap to fail, but it passed (gap: $PENDING_GAP)"
		else
			GAPS=$((GAPS + 1))
			GAP_NAMES+=("$name: $PENDING_GAP")
			echo "GAP  $name: $PENDING_GAP"
		fi
	elif [ "$ok" = 1 ]; then
		PASSED=$((PASSED + 1))
		echo "ok   $name"
	else
		FAILED=$((FAILED + 1))
		FAILED_NAMES+=("$name")
		echo "FAIL $name"
		printf '%s\n' "$detail" | sed 's/^/     /'
	fi
	PENDING_GAP=""
}

describe() {
	printf '     rc=%s\n     stdout: %s\n     stderr: %s\n' "$RC" "$OUT" "$ERR"
}

# expect NAME EXPECTED_STDOUT CMD...: exit status 0 and stdout exactly EXPECTED_STDOUT.
expect() {
	local name="$1" want="$2"
	shift 2
	run "$@"
	if [ "$RC" = 0 ] && [ "$OUT" = "$want" ]; then
		record "$name" 1 ""
	else
		record "$name" 0 "$(describe)
     wanted: $want"
	fi
}

# expect_has NAME NEEDLE CMD...: exit status 0 and NEEDLE in stdout or stderr.
expect_has() {
	local name="$1" needle="$2"
	shift 2
	run "$@"
	if [ "$RC" = 0 ] && [[ "$OUT
$ERR" == *"$needle"* ]]; then
		record "$name" 1 ""
	else
		record "$name" 0 "$(describe)
     wanted to contain: $needle"
	fi
}

# expect_err NAME NEEDLE CMD...: non-zero exit status and NEEDLE in stdout or stderr.
expect_err() {
	local name="$1" needle="$2"
	shift 2
	run "$@"
	if [ "$RC" != 0 ] && [[ "$OUT
$ERR" == *"$needle"* ]]; then
		record "$name" 1 ""
	else
		record "$name" 0 "$(describe)
     wanted failure containing: $needle"
	fi
}

# expect_ok NAME CMD...: exit status 0.
expect_ok() {
	local name="$1"
	shift
	run "$@"
	if [ "$RC" = 0 ]; then
		record "$name" 1 ""
	else
		record "$name" 0 "$(describe)"
	fi
}

# Find a client binary by name: PATH first, then the version-suffixed names and the keg-only
# Homebrew prefixes. Prints the path, or nothing.
find_tool() {
	local name="$1" candidate dir
	if command -v "$name" >/dev/null 2>&1; then
		command -v "$name"
		return
	fi
	for dir in /opt/homebrew/bin /usr/local/bin /opt/homebrew/opt/libpq/bin /usr/local/opt/libpq/bin \
		/opt/homebrew/opt/mysql-client/bin /usr/local/opt/mysql-client/bin /usr/lib/postgresql/*/bin; do
		if [ -x "$dir/$name" ]; then
			echo "$dir/$name"
			return
		fi
		for candidate in "$dir/$name"-[0-9]*; do
			if [ -x "$candidate" ]; then
				echo "$candidate"
				return
			fi
		done
	done
}

# require_tools GROUP TOOL...: export TOOL as a path variable (psql -> PSQL). Missing tools skip
# the group locally and fail the run on CI.
require_tools() {
	local group="$1" missing="" tool path var
	shift
	for tool in "$@"; do
		path="$(find_tool "$tool")"
		if [ -z "$path" ]; then
			missing="$missing $tool"
			continue
		fi
		var="$(echo "$tool" | tr '[:lower:]' '[:upper:]')"
		printf -v "$var" '%s' "$path"
		export "${var?}"
	done
	if [ -n "$missing" ]; then
		if [ -n "${CI:-}" ]; then
			echo "ERROR: $group client tools missing on CI:$missing" >&2
			exit 1
		fi
		echo "SKIP $group: client tools not installed ($missing); install them to run this group" >&2
		return 1
	fi
}

summary() {
	echo
	echo "passed: $PASSED, known gaps: $GAPS, failed: $FAILED"
	if [ "$GAPS" -gt 0 ]; then
		echo "known gaps:"
		printf '  - %s\n' "${GAP_NAMES[@]}"
	fi
	if [ "$FAILED" -gt 0 ]; then
		echo "failures:"
		printf '  - %s\n' "${FAILED_NAMES[@]}"
		return 1
	fi
}
