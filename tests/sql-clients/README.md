# SQL client suite

Drives the real PostgreSQL and MySQL command-line clients against a `surreal` server that serves both wire listeners, once without TLS and once with it.

```bash
cargo build --locked --no-default-features --features storage-mem,http,postgres,mysql
tests/sql-clients/run.sh [pg|my|all]
# or, as CI does it
cargo make ci-sql-clients
```

Clients exercised: `psql`, `pg_isready`, `pg_dump`, `pg_restore`, `pgbench`, `mysql`, `mysqladmin`, `mysqldump`, `mysqlslap`.

- Locally a missing client skips its group with a message; with `CI` set a missing client fails the run.
- `SURREAL_BIN` selects the server binary and `SQL_CLIENTS_BASE_PORT` the first of six consecutive ports.
- Assertions use stable output forms (`psql -tA`, `mysql -B -N`).
- `gap "reason"` before a case marks a known client-visible gap. The case must keep failing; when it starts passing the run fails so the marker is removed. The run prints every open gap.
- `pgbench` and `pg_restore` ship with the `postgresql` server package on Debian and Ubuntu.
- The `mysql_native_password` cases run only with clients that still ship that plugin (older than 9.x).
