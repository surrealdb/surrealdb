# Direct runtime expression lowering

`surrealdb_expr::parser` materializes parser-next's flat arena directly as runtime
expressions. Requests, RPC statement builders, imports, stored catalog clauses
and revisioned expression text share this lowering. Settings, nesting limits and
rendered diagnostics use
the parser-next configuration and diagnostic helpers.

Request parsing leaves passwords and LIVE identities unprepared. Execution
prepares credentials when it reaches a statement, including nested definitions.
Log rendering redacts credentials without hashing them. Durable expression text
contains derived credentials and never stores pending plaintext passwords.

Canonical printers borrow runtime expressions. Catalog INFO, schema definitions
and subscription output render runtime nodes without constructing SQL trees.
Public catalog literals preserve nested sets, record-id keys and canonical range
spelling. User metadata renders credential markers without deriving or exposing
password-equivalent material. Stored clauses preserve statement parentheses,
expression precedence, escaping and geometry formatting. Logical
lowering accepts runtime expressions; SQL frontends retain conversion adapters.

Differential tests compare runtime trees, compact and pretty canonical output,
and diagnostics with the SQL representation. Allocation tests charge allocations
and reallocations on the parsing thread after warming arena caches. Streaming
and settings tests cover chunk boundaries, UTF-8 and nesting limits.

The SQL crates remain dependencies for syntax vocabulary and compatibility
frontends; catalog rendering uses runtime expressions. A WASM size claim requires
comparing identical build profiles and reporting raw and compressed artifact
sizes separately.
