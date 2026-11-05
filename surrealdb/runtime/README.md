# surrealdb-runtime

SurrealQL's pure function library: the families whose result depends on nothing
but their arguments — `math`, `string`, `time`, `crypto`, `encoding`, `geo`,
`vector` and the rest — together with the argument coercion and operator
implementations that every call site needs.

What is *not* here is anything that needs the engine to answer. A function that
reads a record, opens a socket, runs a script, or invokes a user closure needs a
transaction, an HTTP client, a JavaScript runtime or an evaluator, and so lives
above this crate with the services it depends on. Both halves are registered
into the same function registry; a caller cannot tell them apart.

**This crate is an internal implementation detail of SurrealDB.** Its API is
unstable and changes without notice; depend on the `surrealdb` SDK or
`surrealdb-core` instead.
