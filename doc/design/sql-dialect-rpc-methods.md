# The `pgsql` and `mysql` RPC methods

The `pgsql` and `mysql` methods run PostgreSQL and MySQL text on an RPC session, as `query` runs SurrealQL and `gql` runs GQL. They are the contract a SQL client library (a node-postgres-shaped or mysql2-shaped driver over WebSocket or HTTP) has with an instance. The in-process wire listeners (`pgwire`, `mywire`, `serve()`) are a separate path and do not use these methods.

This document is normative.

## Gates

Each method passes all of these on every call; the first failure refuses the call with a `NotAllowed` RPC error that names what denied it.

1. The RPC method allow and deny lists (`--allow-rpc` / `--deny-rpc` with `pgsql` or `mysql`). `describe`, batches and every option are served by the same method name, so one deny entry covers all forms.
2. The per-subject arbitrary-query gate, as for `query`.
3. The `postgres` / `mysql` route capability (`--deny-http postgres` / `mysql`), checked by the datastore for the dialect itself, on a run and on a `describe`.

`query`, `run` and `eval::` take SurrealQL only; no other method reaches a dialect front end. `describe` is gated identically to a run, and its catalog is read through the session, so a subject is told only what its permissions let it read. Authentication, permissions, namespace, database and variables are the session's, exactly as for `query`.

## Sessions and transport

The namespace and database are the session's, chosen with `use` (or the transport's `surreal-ns` / `surreal-db` headers).

| Dialect concept | SurrealDB |
|---|---|
| PostgreSQL database (startup `database`) | the session database, selected with `use` |
| PostgreSQL schema, `search_path` | not mapped; a namespace or database is never selected from text |
| MySQL `USE db` | switches the session database inside the session namespace, through the `use` method and its tenant check; a principal cannot reach a database it could not select with `use` |

WebSocket sessions are connection-scoped and may hold an interactive transaction. HTTP requests may attach to a stored session that keeps its namespace, database and variables across requests and nodes, but hold no open transaction between requests: a dialect `BEGIN` over HTTP is an `ERR` statement entry, and atomic work over HTTP uses a batch with `atomic: true`, which needs no session state. A server-side transaction store is a possible later addition.

## Client contract

All examples show the JSON encoding.

### Request

`pgsql` and `mysql` take positional params `[statements, vars?, options?]`.

- `statements` is a string, or an array of strings and `{ "text": string, "vars"?: object }` objects (a batch). Entries run in order. A string may hold several `;`-separated statements.
- `vars` is an object of variables layered over the session's. A positional parameter (`$1` for PostgreSQL, the n-th `?` for MySQL) binds to the variable `_n`: `{ "_1": "Tobie" }`. In a batch the call's `vars` apply to every entry and an entry's own `vars` win.
- `options` is an object, every field optional:

| Field | Values | Meaning |
|---|---|---|
| `describe` | boolean | Describe the last statement of each entry instead of running it. |
| `rowMode` | `"object"` (default), `"array"` | Rows keyed by column name, or arrays in column order. |
| `valueMode` | `"native"` (default), `"text"` | Native values as the engine returns them, or every value as its text-protocol form (`null` for SQL NULL). |
| `atomic` | boolean, default `false` | Run every statement of the call in one transaction that commits only if all succeed. |

An unknown option, or an unknown value, is `invalid_params`. The envelope `txn` field is as for `query` (WebSocket only).

```json
{ "id": 1, "method": "pgsql", "params": ["SELECT id, name FROM person WHERE name = $1", { "_1": "Tobie" }] }
{ "id": 2, "method": "mysql", "params": ["SELECT name FROM person WHERE name = ?", { "_1": "Tobie" }, { "rowMode": "array" }] }
{ "id": 3, "method": "pgsql", "params": ["SELECT name FROM person WHERE name = $1", null, { "describe": true }] }
{ "id": 4, "method": "pgsql", "params": [["BEGIN", { "text": "SELECT 1" }, "COMMIT"]] }
```

### Response

A call that reaches the engine succeeds with one object:

```json
{
  "statements": [ <statement entry>, ... ],
  "params": [ <parameter descriptor>, ... ],
  "transaction": { "status": "idle" }
}
```

- `statements` has one entry per SQL statement, in order, however many engine statements it lowers to. Within one string, the first failing statement stops the ones after it, which answer with an `ERR` entry that was not run (as in a PostgreSQL simple query or a MySQL multi-statement text); text that does not parse is rejected whole, as one `ERR` entry. A failing batch entry does not stop the entries after it, so independent statements go in separate batch entries.
- `params` is present only for a `describe` call: one descriptor per parameter of the last described statement.
- `transaction.status` is `"idle"`, `"active"` or `"failed"`, and is present on every response.

A statement entry that ran:

```json
{
  "status": "OK",
  "time": "412µs",
  "command": { "tag": "SELECT 1", "rows_affected": 1, "last_insert_id": null, "warnings": 0 },
  "columns": [ { "name": "name", "type": { "name": "text", "oid": 25 }, "nullable": null, "table": "person", "column": "name" } ],
  "rows": [ { "name": "Tobie" } ]
}
```

- `columns` lists the result's columns in projection order, each with its wire type (`type` is `{ "name", "oid" }` for `pgsql` and `{ "name", "mysql_type" }` for `mysql`), `nullable` (always `null`), and `table` and `column` giving the origin of a plain table column, else `null`. When the planner can list the columns before the statement runs they are its own; for a `*` projection they are those of the first row in name order, typed from their values (`text`, `bigint`, `double precision`, `numeric`, `boolean`, `bytea`, `timestamp with time zone`, `interval`, `uuid`, `json`). With `rowMode: "array"` each row is an array in `columns` order.
- `command.tag` is the dialect's command tag: PostgreSQL `SELECT n`, `INSERT 0 n`, `UPDATE n`, `DELETE n`, `CREATE TABLE`, `BEGIN`, `COMMIT`, `ROLLBACK`; MySQL the statement kind (`SELECT`, `INSERT`, `REPLACE`, `UPDATE`, `DELETE`, `CREATE TABLE`, `USE`). `rows_affected` is the row count of a read or the number of records an `INSERT`, `UPDATE` or `DELETE` wrote, and `null` for statements that have no count. `last_insert_id` is always `null` and `warnings` always `0`.
- An `INSERT`, `UPDATE` or `DELETE` answers with a count and empty `columns` and `rows`, except a PostgreSQL one with a `RETURNING` clause, which answers with the rows it wrote.

A `describe` entry has `columns` with a type per column and no `rows`:

```json
{
  "statements": [
    { "status": "OK", "columns": [ { "name": "name", "type": { "name": "text", "oid": 25 }, "nullable": null, "table": "person", "column": "name" } ] }
  ],
  "params": [ { "type": { "name": "text", "oid": 25 } } ],
  "transaction": { "status": "idle" }
}
```

`type` is the dialect's wire type as the listener reports it: `{ "name", "oid" }` for `pgsql`, `{ "name", "mysql_type" }` (the protocol type code) for `mysql`. `nullable` is `null` (unknown). `table` and `column` give the origin of a plain table column, else `null`. Only a `SELECT` has columns to describe; any other statement describes with `columns: []`. A projection whose columns are known only from the rows describes with `columns: null`.

### Errors

An error that belongs to a statement (a failed write, a parse or planning failure) is an `ERR` entry; the call itself succeeds:

```json
{
  "status": "ERR",
  "error": { "message": "…", "severity": "ERROR", "sqlstate": "42601", "errno": null }
}
```

`sqlstate` is a five-character SQLSTATE and `errno` is `null` for PostgreSQL; for MySQL `errno` is the error number. Parse failures are `42601` (MySQL `42000` / `1064`), permission failures `42501` (MySQL `42000` / `1142`), a cancelled statement `57014` (MySQL `70100` / `1317`), a statement refused because the transaction failed `25P02`, an unknown setting `42704`, and a `describe` that fails planning carries the planner's own code. Any other failure is `XX000` (MySQL `HY000` / `1105`). `position` is the 1-based character offset a parse error names, else `null`.

Errors that are not about statement text fail the call as an RPC error and carry no `sqlstate`: a denied method or capability (`NotAllowed`), malformed params (`invalid_params`), an unknown session, a deadline.

### Transactions

Over WebSocket the session holds at most one transaction.

- An entry whose whole text is `BEGIN`, `BEGIN WORK`, `BEGIN TRANSACTION` or `START TRANSACTION` opens it; `COMMIT`, `COMMIT WORK`, `COMMIT TRANSACTION`, `END`, `END TRANSACTION` commit it; `ROLLBACK`, `ROLLBACK WORK`, `ROLLBACK TRANSACTION`, `ABORT` roll it back. A control statement is recognised only as an entry of its own (a call whose text is only that statement, or one batch entry); inside a longer text it is not.
- Later `pgsql` / `mysql` calls on the session run inside the open transaction without an envelope `txn`. The RPC `begin` / `commit` / `cancel` methods and the envelope `txn` field remain available.
- `transaction.status` is `"active"` while a transaction is open. For `pgsql`, a failing statement makes it `"failed"`: every later statement is refused with `25P02` until the entry that ends the transaction, and `COMMIT` of a failed transaction rolls it back. MySQL has no failed state and stays `"active"`.
- The open transaction is recorded in the session variables `sql_txn` and `sql_txn_failed`, which statements of the session can therefore read.
- A call with no statements and the `describe` option unset returns no entries and the current `transaction` status.

Over HTTP the status is `"idle"` and a transaction-control entry is an `ERR` entry.

### Atomic calls

With `atomic: true` every statement of the call runs in one transaction of its own, whatever the transport. It commits if all statements succeed and is cancelled otherwise. The response has no `params`, and `transaction` is `{ "status": "idle", "committed": true | false }`. After the first failing statement the statements that follow are not run and answer `25P02`; the entries before it keep their `OK` status but were rolled back, which `committed: false` reports. A session that holds a transaction refuses the call with `invalid_params`, and a transaction-control or `SET` entry in an atomic call is an `ERR` entry (`25001`, MySQL `1400`).

### Session

An entry whose whole text is MySQL `USE name` switches the session database as the `use` method does and persists for the session.

PostgreSQL `SET [SESSION | LOCAL] name = value`, `SHOW name` and `RESET name | ALL` act on session settings, which are session variables named `sql_setting_<name>` (lower-cased, non-alphanumerics as `_`). `SHOW` answers one row and one `text` column named after the setting, with the PostgreSQL default for the common settings (`timezone`, `application_name`, `search_path`, `client_encoding`, `datestyle`, `standard_conforming_strings`, `extra_float_digits`, `intervalstyle`, `bytea_output`, `statement_timeout`, `client_min_messages`) and `42704` for others. `SET LOCAL` is treated as `SET`. Settings last for the session over WebSocket and for the request over HTTP, so an HTTP client sends the `SET` statements it needs in the same call. A setting has no other effect on the engine; `search_path` never selects a namespace or database. MySQL `SET` statements are accepted and have no effect.

### Cancel

A `pgsql` or `mysql` call over WebSocket is cancelled with `query_cancel` addressed by the call's request `id`, as for streaming queries. The statement it is running answers with the cancellation error and the statements after it do not run. A request id with no such call in flight is `stream_not_found`. Over HTTP a client cancels by closing the request.

### Value encoding

Native row values are SurrealDB values in the transport's encoding, exactly as `query` returns them (JSON or CBOR, by the transport's negotiated format); `columns[].type` says how to decode each. With `valueMode: "text"` every value is a string, or `null` for SQL NULL: booleans are `t` / `f` (MySQL `1` / `0`), numbers are their decimal text, bytes are `\x` hex (PostgreSQL), datetimes are `YYYY-MM-DD HH:MM:SS[.f]+00` (MySQL without the offset), durations are `[N days ]HH:MM:SS`, arrays of a non-JSON type are PostgreSQL array literals, and objects and JSON-typed values are JSON text.
