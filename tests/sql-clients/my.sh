#!/usr/bin/env bash
# MySQL client suite: mysql, mysqladmin, mysqldump, mysqlslap against the mywire listener.
# Sourced by run.sh, which provides the listener ports, the CA file and lib.sh.

require_tools mysql mysql mysqladmin mysqldump mysqlslap || return 0

MY_DB="t/t"
MY_PW="-proot"

# mysql in script-stable form: batch (tab-separated) output, no headers unless asked, no config
# files. Callers pick the TLS mode and port.
my_plain() { "$MYSQL" --no-defaults -h127.0.0.1 -P"$MY_PORT" -uroot "$MY_PW" --protocol=tcp --ssl-mode=DISABLED -D "$MY_DB" -B "$@" 2>&1 | grep -v 'Using a password'; return "${PIPESTATUS[0]}"; }
my_tls() { "$MYSQL" --no-defaults -h127.0.0.1 -P"$MY_TLS_PORT" -uroot "$MY_PW" --protocol=tcp --ssl-mode=VERIFY_IDENTITY --ssl-ca="$CA_FILE" -D "$MY_DB" -B "$@" 2>&1 | grep -v 'Using a password'; return "${PIPESTATUS[0]}"; }
# Runs against the TLS server with certificate checks off, for cases that do not test TLS itself.
my() { "$MYSQL" --no-defaults -h127.0.0.1 -P"$MY_TLS_PORT" -uroot "$MY_PW" --protocol=tcp --ssl-mode=REQUIRED -D "$MY_DB" -B "$@" 2>&1 | grep -v 'Using a password'; return "${PIPESTATUS[0]}"; }
admin_tls() { "$MYSQLADMIN" --no-defaults -h127.0.0.1 -P"$MY_TLS_PORT" -uroot "$MY_PW" --protocol=tcp --ssl-mode=REQUIRED "$@" 2>&1 | grep -v 'Using a password'; return "${PIPESTATUS[0]}"; }

# Multi-command cases run in `bash -c`, which sees the helper through the exported function.
export -f my
export MYSQL MY_TLS_PORT MY_PW MY_DB

echo "== MySQL clients: $("$MYSQL" --version), $("$MYSQLADMIN" --version), $("$MYSQLDUMP" --version), $("$MYSQLSLAP" --version)"

# --- readiness and connection ---
expect_has "mysqladmin ping over tls" "mysqld is alive" admin_tls ping
expect "mysql select over tls (verify identity)" "1	x" my_tls -N -e "select 1 as a, 'x' as b"
expect "mysql select with require" "2" my -N -e "select 2"
expect "caching_sha2_password over tls" "3" my_tls --default-auth=caching_sha2_password -N -e "select 3"
expect "caching_sha2_password without tls via public key" "4" my_plain --get-server-public-key -N -e "select 4"

# The mysql_native_password client plugin was removed from the 9.x clients. The server answers a
# native login that misses its fast-authentication cache with a mysql_clear_password switch, which
# it only offers over TLS and which the stock client refuses unless told to enable it.
if "$MYSQL" --no-defaults --default-auth=mysql_native_password --help >/dev/null 2>&1 \
	&& [ "$("$MYSQL" --version | sed -n 's/.*Ver \([0-9]*\)\..*/\1/p')" -lt 9 ]; then
	gap "mysql_native_password without TLS is refused; real MySQL accepts it"
	expect "mysql_native_password without tls" "5" my_plain --default-auth=mysql_native_password -N -e "select 5"
	gap "a native login is answered with a mysql_clear_password switch the stock client refuses"
	expect "mysql_native_password over tls" "6" my --default-auth=mysql_native_password -N -e "select 6"
	expect "mysql_native_password over tls with the cleartext plugin enabled" "7" \
		my --default-auth=mysql_native_password --enable-cleartext-plugin -N -e "select 7"
else
	echo "SKIP mysql_native_password: this mysql client no longer ships that plugin" >&2
fi

expect_err "wrong password" "ERROR 1045 (28000): Access denied for user 'root'" \
	"$MYSQL" --no-defaults -h127.0.0.1 -P"$MY_TLS_PORT" -uroot -pwrong --protocol=tcp --ssl-mode=REQUIRED -D "$MY_DB" -e "select 1"
expect_err "unknown user" "ERROR 1045 (28000)" \
	"$MYSQL" --no-defaults -h127.0.0.1 -P"$MY_TLS_PORT" -unobody -px --protocol=tcp --ssl-mode=REQUIRED -D "$MY_DB" -e "select 1"
expect "database-scoped user" "alice" \
	"$MYSQL" --no-defaults -h127.0.0.1 -P"$MY_TLS_PORT" -ualice -psecret --protocol=tcp --ssl-mode=REQUIRED -D "$MY_DB" -N -e "select 'alice'"
expect_err "read-only user cannot write" "ERROR" \
	"$MYSQL" --no-defaults -h127.0.0.1 -P"$MY_TLS_PORT" -ualice -psecret --protocol=tcp --ssl-mode=REQUIRED -D "$MY_DB" -e "create table forbidden (id int primary key)"

# --- schema and data ---
expect_ok "create table" my -e "create table person (id int primary key, name varchar(20), age int)"
expect_ok "insert rows" my -e "insert into person (id, name, age) values (1, 'a', 30), (2, 'b', 40)"
expect "select ordered" "1	a	30
2	b	40" my -N -e "select id, name, age from person order by id"
expect "select with header" "id	name
1	a" my -e "select id, name from person where id = 1"
expect "update" "31" bash -c "my -e 'update person set age = 31 where id = 1' ; my -N -e 'select age from person where id = 1'"
expect "aggregates" "71	35.5" my -N -e "select sum(age), avg(age) from person"
expect "like filter" "a" my -N -e "select name from person where name like 'a%'"
expect "limit and offset" "2	b" my -N -e "select id, name from person order by id limit 1 offset 1"
expect "delete" "1" bash -c "my -e 'delete from person where id = 2' ; my -N -e 'select count(*) from person'"
expect "utf8 round trip" "é" my -N -e "select 'é'"
expect "null, float and boolean" "1.5	NULL	1" my -N -e "select 1.5, null, true"
expect "union" "1
2" my -N -e "select 1 union select 2"

# --- SHOW, DESCRIBE, USE ---
expect "show databases" "Database
t" my -e "show databases"
expect "show tables" "Tables_in_t
person" my -e "show tables"
expect "describe lists the columns" "age
id
name" bash -c "my -N -e 'describe person' | cut -f1 | sort"
expect "select database()" "t" my -N -e "select database()"
expect "use <db>" "t" my -N -e "use t; select database()"
expect_has "show create table names the table" "person" my -e "show create table person"
expect "show variables like" "sql_mode" bash -c "my -N -e \"show variables like 'sql_mode'\" | cut -f1"
expect_has "select @@version" "." my -N -e "select @@version"

# --- multi-statements, transactions, SET ---
expect "multiple statements in one -e" "1
2" my -N -e "select 1; select 2;"
expect "rollback discards" "1" bash -c "my -e 'start transaction; insert into person (id, name, age) values (3, \"c\", 5); rollback;' ; my -N -e 'select count(*) from person'"
expect "commit keeps" "2" bash -c "my -e 'start transaction; insert into person (id, name, age) values (4, \"d\", 6); commit;' ; my -N -e 'select count(*) from person'"
expect_ok "set autocommit, names and sql_mode" my -e "set autocommit=1; set names utf8mb4; set sql_mode=''"
expect "user variables" "5" my -N -e "set @a = 5; select @a"
expect "string user variable and an unset one" "x'y	NULL" my -N -e "set @s = 'x''y'; select @s, @unset"
expect "user variable from an expression" "7" my -N -e "set @a = 5, @b = @a + 2; select @b"
expect_err "EXECUTE of an unknown statement" "1243" my -e "execute nope"
expect "user variable beside a system variable" "9" my -N -e "set @m = 9, autocommit = 1; select @m"
expect_err "a statement cannot prepare itself" "1295" my -e "prepare p from 'execute p'"
expect_err "a syntax error is refused by PREPARE" "1064" my -e "prepare e from 'selec 1'"
expect_err "DROP PREPARE releases a statement" "1243" my -e "prepare d from 'select 1'; drop prepare d; execute d"
expect_has "a refused replacement leaves no statement" "ERROR 1243" bash -c "printf \"prepare r from 'select 1';\\nprepare r from 'selec 2';\\nexecute r;\\n\" | my -f 2>&1"
expect "PREPARE FROM and EXECUTE USING" "a" my -N -e "prepare s from 'select name from person where age > ? order by id'; set @i = 10; execute s using @i;"
expect "literal column name" "2" bash -c "my -e 'select 2' | head -1"

# --- scripts and errors ---
MY_SCRIPT="$WORKDIR/script.sql"
cat >"$MY_SCRIPT" <<'SQL'
insert into person (id, name, age) values (10, 'script1', 1);
insert into person (id, name, age) values (11, 'script2', 2);
select name from person where id in (10, 11) order by id;
SQL
expect "script on stdin" "script1
script2" bash -c "my -N < '$MY_SCRIPT'"
expect_err "missing table prints errno and SQLSTATE" "ERROR 1146 (42S02) at line 1: The table 'nope' does not exist" my -e "select * from nope"
expect_err "syntax error prints errno 1064" "ERROR 1064 (42000)" my -e "selec 1"
expect_err "an error in a script stops the client" "ERROR 1146" bash -c "printf 'select * from nope;\nselect 1;\n' | my"
expect "--force carries on after an error" "1" bash -c "printf 'select * from nope;\nselect 1;\n' | my -N --force | grep -v ERROR"

# --- large result sets ---
seq 0 49 | awk '{
	printf "insert into person (id, name, age) values "
	for (i = 0; i < 100; i++) {
		id = 100 + $1 * 100 + i
		printf "(%d, '\''big%d'\'', %d)%s", id, id - 100, i, (i == 99 ? ";\n" : ",")
	}
}' >"$WORKDIR/big.sql"
expect_ok "load 5000 rows from a script" bash -c "my < '$WORKDIR/big.sql'"
expect "5000 rows aggregate" "5000" my -N -e "select count(*) from person where name like 'big%'"
expect "5000 rows stream to the client" "5000" bash -c "my -N -e \"select id from person where name like 'big%'\" | wc -l | tr -d ' '"

# --- admin, dump and load tools ---
expect_has "mysqladmin version" "Server version" admin_tls version
expect_has "mysqladmin status" "Uptime" admin_tls status
expect_has "mysqladmin variables" "version" admin_tls variables
expect "the column histogram probe of mysqldump 8.0 finds none" "" my -N -e "SELECT COLUMN_NAME, JSON_EXTRACT(HISTOGRAM, '\$.\"number-of-buckets-specified\"') FROM information_schema.COLUMN_STATISTICS WHERE SCHEMA_NAME = 't/t' AND TABLE_NAME = 'person'"
expect_has "mysqldump of a table" "INSERT INTO \`person\` VALUES" "$MYSQLDUMP" --no-defaults -h127.0.0.1 -P"$MY_TLS_PORT" -uroot "$MY_PW" --protocol=tcp --ssl-mode=REQUIRED --no-tablespaces --skip-lock-tables "$MY_DB" person
expect_has "mysqlslap runs concurrent clients" "Number of clients running queries: 4" \
	"$MYSQLSLAP" --no-defaults -h127.0.0.1 -P"$MY_TLS_PORT" -uroot "$MY_PW" --protocol=tcp --ssl-mode=REQUIRED \
	--create-schema="$MY_DB" --concurrency=4 --iterations=2 --number-of-queries=40 --query="select 1"
