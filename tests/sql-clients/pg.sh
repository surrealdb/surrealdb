#!/usr/bin/env bash
# PostgreSQL client suite: psql, pg_isready, pg_dump, pgbench against the pgwire listener.
# Sourced by run.sh, which provides the listener ports, the CA file and lib.sh.

require_tools postgres psql pg_isready pg_dump pg_restore pgbench || return 0

export PGPASSWORD=root
PG_CONN="host=127.0.0.1 port=$PG_PORT user=root dbname=t/t"
PG_TLS_CONN="host=127.0.0.1 port=$PG_TLS_PORT user=root dbname=t/t sslrootcert=$CA_FILE"

# psql in script-stable form: unaligned, tuples only, no psqlrc.
psq() { "$PSQL" -X -tA "$PG_CONN" "$@"; }
psq_tls() { PGSSLMODE=verify-full "$PSQL" -X -tA "$PG_TLS_CONN" "$@"; }

echo "== PostgreSQL clients: $("$PSQL" --version), $("$PG_ISREADY" --version), $("$PG_DUMP" --version), $("$PGBENCH" --version)"

# --- readiness and connection ---
expect_has "pg_isready plain" "accepting connections" "$PG_ISREADY" -h 127.0.0.1 -p "$PG_PORT"
expect_has "pg_isready tls" "accepting connections" "$PG_ISREADY" -h 127.0.0.1 -p "$PG_TLS_PORT"
expect "psql select over plain" "1|x" psq -c "select 1 as a, 'x' as b"
expect "psql select over tls (verify-full)" "2" psq_tls -c "select 2"
expect "psql sslmode=require" "3" env PGSSLMODE=require "$PSQL" -X -tA "host=127.0.0.1 port=$PG_TLS_PORT user=root dbname=t/t" -c "select 3"
expect "psql scram-sha-256 plain" "ok" env PGREQUIREAUTH=scram-sha-256 "$PSQL" -X -tA "$PG_CONN" -c "select 'ok'"
expect "psql scram-sha-256 tls" "ok" env PGREQUIREAUTH=scram-sha-256 PGSSLMODE=require "$PSQL" -X -tA "$PG_TLS_CONN" -c "select 'ok'"
expect_err "psql wrong password" 'password authentication failed for user "root"' env PGPASSWORD=wrong "$PSQL" -X "$PG_CONN" -c "select 1"
expect_err "psql unknown user" 'password authentication failed for user "nobody"' env PGPASSWORD=x "$PSQL" -X "host=127.0.0.1 port=$PG_PORT user=nobody dbname=t/t" -c "select 1"
expect "psql database-scoped user" "alice" env PGPASSWORD=secret "$PSQL" -X -tA "host=127.0.0.1 port=$PG_PORT user=alice dbname=t/t" -c "select 'alice'"
expect_err "psql read-only user cannot write" "ERROR" env PGPASSWORD=secret "$PSQL" -X "host=127.0.0.1 port=$PG_PORT user=alice dbname=t/t" -c "create table forbidden (id text primary key)"

# --- schema and data ---
expect "create table" "CREATE TABLE" psq -c "CREATE TABLE item (id TEXT PRIMARY KEY, name TEXT, qty INT)"
expect "insert rows" "INSERT 0 2" psq -c "INSERT INTO item (id, name, qty) VALUES ('a', 'apple', 3), ('b', 'pear', 5)"
expect "select ordered" "a|apple|3
b|pear|5" psq -c "select id, name, qty from item order by id"
expect "update" "UPDATE 1" psq -c "update item set qty = qty + 1 where id = 'a'"
expect "select after update" "4" psq -c "select qty from item where id = 'a'"
expect "delete" "DELETE 1" psq -c "delete from item where id = 'b'"
expect "aggregate" "1" psq -c "select count(*) from item"
expect "csv output" "id,name,qty
a,apple,4" env PGPASSWORD=root "$PSQL" -X --csv "$PG_CONN" -c "select id, name, qty from item"
expect "expanded output" "-[ RECORD 1 ]
id   | a
name | apple" env PGPASSWORD=root "$PSQL" -X -x "$PG_CONN" -c "select id, name from item"
expect "null display" "(null)" psq -P null="(null)" -c "select null"
expect "multiple -c commands" "1
2" psq -c "select 1" -c "select 2"
expect "multi-statement in one -c" "1
2" psq -c "select 1; select 2"
expect_has "command tag for insert" "INSERT 0 1" "$PSQL" -X "$PG_CONN" -c "insert into item (id, name, qty) values ('c', 'cherry', 1)"

# --- scripts ---
PG_SCRIPT="$WORKDIR/script.sql"
cat >"$PG_SCRIPT" <<'SQL'
insert into item (id, name, qty) values ('s1', 'script1', 10);
insert into item (id, name, qty) values ('s2', 'script2', 20);
select name from item where id in ('s1', 's2') order by id;
SQL
expect "psql -f script" "script1
script2" env PGPASSWORD=root "$PSQL" -X -tA -q "$PG_CONN" -f "$PG_SCRIPT"
expect "psql script on stdin" "2" bash -c "echo 'select 2;' | '$PSQL' -X -tA '$PG_CONN'"
expect_err "psql ON_ERROR_STOP halts the script" "does not exist" "$PSQL" -X -v ON_ERROR_STOP=1 "$PG_CONN" -c "select * from missing_table" -c "select 1"

# --- errors ---
expect_err "missing table error" 'The table '"'"'nope'"'"' does not exist' "$PSQL" -X "$PG_CONN" -c "select * from nope"
expect_err "missing table sqlstate" "42P01" "$PSQL" -X -v VERBOSITY=sqlstate "$PG_CONN" -c "select * from nope"
expect_err "syntax error is reported" "ERROR" "$PSQL" -X "$PG_CONN" -c "selec 1"
expect_err "syntax error sqlstate" "42601" "$PSQL" -X -v VERBOSITY=sqlstate "$PG_CONN" -c "selec 1"

# --- transactions ---
PG_TX="$WORKDIR/tx.sql"
cat >"$PG_TX" <<'SQL'
begin;
insert into item (id, name, qty) values ('tx1', 'tx', 1);
rollback;
select count(*) from item where id = 'tx1';
begin;
insert into item (id, name, qty) values ('tx2', 'tx', 2);
commit;
select count(*) from item where id = 'tx2';
SQL
expect "rollback discards, commit keeps" "0
1" env PGPASSWORD=root "$PSQL" -X -tA -q "$PG_CONN" -f "$PG_TX"

# --- SET / SHOW ---
expect "set and show application_name" "SET
suite" psq -c "set application_name = 'suite'; show application_name"
expect "show server_version" "16.0-surrealdb" psq -c "show server_version"
expect "show server_version_num" "160000" psq -c "show server_version_num"
expect "current_database and current_user" "t|root" psq -c "select current_database(), current_user"
expect "search_path" "SET
public" psq -c "set search_path to public; show search_path"
expect_has "select version()" "PostgreSQL 16.0" "$PSQL" -X -tA "$PG_CONN" -c "select version()"

# --- meta-commands ---
expect "\\dt lists tables" "public|item|table|root" psq -c '\dt'
expect "\\dn lists schemas" "public|root" psq -c '\dn'
expect_has "\\du lists roles" "root" "$PSQL" -X -tA "$PG_CONN" -c '\du'
expect_has "\\di lists indexes" "item_pkey" "$PSQL" -X -tA "$PG_CONN" -c '\di'
gap "psql \\d <table> reaches its publication query, which reads generate_series in FROM with an argument of the outer query"
expect_has "\\d item describes the table" "qty" "$PSQL" -X "$PG_CONN" -c '\d item'
expect_has "\\l lists databases" "UTF8" "$PSQL" -X "$PG_CONN" -c '\l'

# --- extended protocol: \bind from stdin ---
expect "\\bind with an integer parameter" "42" bash -c "printf 'select \$1::int + 1 \\\\bind 41 \\\\g\n' | '$PSQL' -X -tA '$PG_CONN'"
expect "\\bind with a text parameter" "pear" bash -c "
	'$PSQL' -X -tA '$PG_CONN' -c \"insert into item (id, name, qty) values ('b', 'pear', 5)\" >/dev/null
	printf 'select name from item where id = \$1 \\\\bind b \\\\g\n' | '$PSQL' -X -tA '$PG_CONN'"
expect "\\bind with two parameters" "13" bash -c "printf 'select qty + \$1::int from item where id = \$2 \\\\bind 9 a \\\\g\n' | '$PSQL' -X -tA '$PG_CONN'"
expect "PREPARE and EXECUTE" "PREPARE
pear
DEALLOCATE" bash -c "printf '%s\n' \"PREPARE s AS select name from item where id = \\\$1;\" \"EXECUTE s('b');\" \"DEALLOCATE s;\" | '$PSQL' -X -tA '$PG_CONN'"

# --- COPY ---
expect "\\copy to stdout as csv" "id,name,qty
a,apple,4
b,pear,5" bash -c "'$PSQL' -X -q '$PG_CONN' -c '\\copy (select id, name, qty from item where id in (\$\$a\$\$, \$\$b\$\$) order by id) to stdout csv header'"
PG_CSV="$WORKDIR/in.csv"
printf 'id,name,qty\nc1,fig,9\nc2,date,8\n' >"$PG_CSV"
expect "\\copy from csv" "COPY 2" env PGPASSWORD=root "$PSQL" -X -tA "$PG_CONN" -c "\\copy item from '$PG_CSV' csv header"
expect "copied rows are queryable" "fig
date" psq -c "select name from item where id in ('c1', 'c2') order by id"
gap "aggregate functions do not accept an ORDER BY clause inside the call"
expect "string_agg with ORDER BY" "fig|date" psq -c "select string_agg(name, '|' order by id) from item where id in ('c1', 'c2')"

# --- large result sets ---
echo "id,name,qty" >"$WORKDIR/big.csv"
seq 0 4999 | awk '{print "big"$1",name"$1","$1}' >>"$WORKDIR/big.csv"
expect "\\copy of 5000 rows" "COPY 5000" env PGPASSWORD=root "$PSQL" -X -tA "$PG_CONN" -c "\\copy item from '$WORKDIR/big.csv' csv header"
expect "5000 rows aggregate" "5000|12497500" psq -c "select count(*), sum(qty) from item where id like 'big%'"
expect "5000 rows stream to the client" "5000" bash -c "'$PSQL' -X -tA '$PG_CONN' -c \"select id from item where id like 'big%'\" | wc -l | tr -d ' '"
expect "generate_series" "5" bash -c "'$PSQL' -X -tA '$PG_CONN' -c 'select generate_series(1, 5)' | wc -l | tr -d ' '"

# --- dump and benchmark tools ---
gap "pg_dump's catalog queries are not served"
expect_ok "pg_dump" "$PG_DUMP" "$PG_CONN"
gap "pg_restore has no archive to read while pg_dump fails"
expect_ok "pg_dump custom format restores listing" bash -c "'$PG_DUMP' -Fc '$PG_CONN' -f '$WORKDIR/db.dump' && '$PG_RESTORE' --list '$WORKDIR/db.dump'"
expect_ok "pgbench -i without data generation" env PGPASSWORD=root "$PGBENCH" -h 127.0.0.1 -p "$PG_PORT" -U root -i -I dtpv t/t
gap "pgbench -i loads its tables with COPY and no column list, and the catalog does not record the order the columns were declared in"
expect_ok "pgbench -i initialises" env PGPASSWORD=root "$PGBENCH" -h 127.0.0.1 -p "$PG_PORT" -U root -i t/t

PGB_SCRIPT="$WORKDIR/bench.sql"
cat >"$PGB_SCRIPT" <<'SQL'
\set aid random(1, 100000)
select qty from item where id = 'a';
update item set qty = qty + 1 where id = 'a';
SQL
expect_has "pgbench custom script runs without errors" "number of transactions actually processed: 40/40" \
	"$PGBENCH" -h 127.0.0.1 -p "$PG_PORT" -U root -n -f "$PGB_SCRIPT" -c 2 -t 20 t/t
