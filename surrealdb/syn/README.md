# surrealdb-syn

The SurrealQL lexer and parser: text in, [`surrealdb-sql`](../sql) AST out.

This crate is an internal implementation detail of SurrealDB. It carries no
stability guarantee and may change or disappear in any release. Depend on
`surrealdb` or `surrealdb-core` instead.

## Layer

```
surrealdb-sql   (the AST this crate produces)
      ^
surrealdb-syn   (this crate: lexer + parser)
      ^
surrealdb-core  (translates capabilities into settings, lowers the AST)
```

The crate parses against an explicit [`ParserSettings`]; it does not know about
capabilities, datastore configuration, or the engine's error type. Core owns
that translation and keeps the `surrealdb_core::syn::parse*` entry points, so
callers see the same API they always did.

## Errors

Parsing fails with `ParseError`: either the input exceeded the addressable
`u32` span space, or a `RenderedError` carrying the annotated source snippet.
Core maps both onto its own error enum, so user-facing messages are unchanged.

## Lifetime

This crate is scheduled for deletion once the greenfield parser
(`surrealdb-ast` + `surrealdb-parser`) reaches parity and takes over the same
layer. Do not grow its API surface.
