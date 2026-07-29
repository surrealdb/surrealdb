# surrealdb-sql

The abstract syntax tree for SurrealQL: the shape a query has immediately after
parsing, before any lowering, name resolution, or planning.

This crate is an internal implementation detail of SurrealDB. It carries no
stability guarantee and may change or disappear in any release. Depend on
`surrealdb` or `surrealdb-core` instead.

## Layer

```
surrealdb-sql  (this crate: the AST)
      ^
surrealdb-syn  (the parser that builds it)
      ^
surrealdb-core (lowers the AST into the expression layer and executes it)
```

Everything this crate depends on sits below it: `surrealdb-strand`,
`surrealdb-common`, `surrealdb-types`, `surrealdb-iam`, and upstream scalar
crates (`chrono`, `uuid`, `rust_decimal`, `regex`, `bytes`, `geo-types`). It
never reaches up into the parser, the expression layer, the catalog, or the
engine.

## Scope

The AST is parse-only. No node here is persisted, so no node carries a
storage-format guarantee; `#[revisioned]` appears only where a type is shared
with a stored twin that lives elsewhere.

Lowering lives with the consumer, not here. `surrealdb-core` owns the
`sql -> expr` conversions in its `expr::convert` module, which keeps the
direction of every dependency pointing down.

## Lifetime

This crate is scheduled for deletion once the greenfield parser
(`surrealdb-ast` + `surrealdb-parser`) reaches parity and takes over the same
layer. Do not grow its API surface.
