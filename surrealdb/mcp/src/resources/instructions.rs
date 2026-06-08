//! Server instructions resource.

pub fn get_instructions() -> &'static str {
	r#"# SurrealDB MCP Server

This MCP server provides direct access to a SurrealDB database instance.

## Available Tools

### Database Operations
- **query**: Execute raw SurrealQL queries with parameterized inputs ($param syntax)
- **run**: Invoke a SurrealQL function (built-in or user-defined) with typed argument bindings
- **select**: Query records with filters, sorting, and pagination
- **create**: Create new records with content data (bound via $data)
- **insert**: Bulk insert records into a table (bound via $data)
- **upsert**: Create or update records with CONTENT/MERGE/PATCH modes
- **update**: Update existing records with CONTENT/MERGE/PATCH modes
- **delete**: Remove records with optional WHERE clause
- **relate**: Create graph relationships between records

### Schema Introspection
- **info**: Dump full schema for a scope ('root', 'ns', 'db', or a table name). Use when you want everything at once.
- **list**: Enumerate entities of a single kind (namespaces, databases, tables, fields, indexes, events, functions, params, analyzers, apis, buckets, models, modules, sequences, configs, users, accesses, nodes). Set `table` for fields/indexes/events; set `scope` for users/accesses.

### Context
- **use**: Switch the active namespace and/or database. Provide `namespace`, `database`, or both in one call.

## Available Resources
- `surrealdb://instructions` -- this document
- `surrealdb://info` -- server name, version, and protocol
- `surrealdb://version` -- short version string

### Resource templates (require expansion)
- `surrealdb://schema/ns/{namespace}/db/{database}` -- full schema for a specific database, including per-table fields, indexes, and events in a single fetch
- `surrealdb://schema/ns/{namespace}/db/{database}/table/{table}` -- schema for a specific table

Schema URIs embed the target `{namespace}` / `{database}` / `{table}`
explicitly so the URI is globally unique and safe to cache across `use`
switches. Resolve the template by substituting the identifiers (e.g.
`surrealdb://schema/ns/acme/db/prod/table/users`).

The database-level resource enriches every table with its per-table
schema in one call, capped at `SURREAL_MCP_SCHEMA_RESOURCE_MAX_TABLES`
tables. If the cap fires, untouched tables retain their bare
`DEFINE TABLE` string and the response includes a
`tables_truncated_at` marker.

## Available Prompts
- **query_builder**: Guided help building SurrealQL queries
- **schema_explorer**: Explore and understand the database schema
- **data_modeler**: Design tables, fields, and relationships
- **transaction_guide**: Multi-statement transactions with BEGIN/COMMIT
- **graph_traversal**: Graph queries, relationships, and traversals
- **search_guide**: Full-text search with analyzers and indexes

## SurrealQL Quick Reference

SurrealDB uses SurrealQL, supporting:
- Document operations: SELECT, CREATE, INSERT, UPDATE, UPSERT, DELETE
- Graph traversals: RELATE, `->edge->target`, `<-edge<-source` syntax
- Transactions: BEGIN TRANSACTION / COMMIT TRANSACTION / CANCEL TRANSACTION
- Subqueries, parameterized queries ($param syntax)
- Schema definitions: DEFINE TABLE, DEFINE FIELD, DEFINE INDEX, DEFINE EVENT
- Full-text search: DEFINE ANALYZER, DEFINE INDEX ... SEARCH, @@ operator
- Access control: DEFINE ACCESS, PERMISSIONS
- Functions: Built-in (math::, string::, array::, time::, crypto::) and custom (DEFINE FUNCTION)

## Tips
- Always use parameterized queries via the `query` tool for dynamic values
- Use `list` to enumerate one kind at a time; use `info` when you need the full picture of a scope or table
- Data values in CRUD tools (create, insert, update, upsert, relate) are automatically bound as typed variables
- Graph relationships are created with RELATE and traversed with arrow syntax
- Use `run` for one-off function calls (`math::sum`, `fn::my_function`) without crafting a full query

## Typed values via `$ql` (decimal, datetime, duration, record id, uuid, ...)

JSON has no native form for SurrealDB's richer scalar types, so the
structured CRUD tools (`create`, `insert`, `upsert`, `update`, `relate`)
and `run` accept a `$ql` sentinel object anywhere in the input tree.
The sentinel's body is a SurrealQL expression parsed under the same
recursion limits as the `query` tool; the resulting value is bound
verbatim, so coercion rules in your schema fire normally.

```json
{
  "price":     { "$ql": "9.99dec" },
  "placed_at": { "$ql": "d'2026-04-27T11:40:00Z'" },
  "wait":      { "$ql": "1h30m" },
  "customer":  { "$ql": "customer:alice" },
  "id":        { "$ql": "u'01933a3c-...'" }
}
```

Rules:

- The sentinel object must contain `$ql` as its only key.
- The body must be a non-empty string within the configured byte cap.
- The string must parse as a single SurrealQL value (not a statement);
  use the raw `query` tool for anything more complex.
- Prefer the SurrealQL form everywhere it makes sense -- it is the
  canonical, lossless way to express records, decimals, durations,
  datetimes, and uuids in tool inputs.
"#
}
