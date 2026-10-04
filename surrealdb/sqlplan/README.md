# surrealdb-sqlplan

SQL planner, binder, and bound IR for SurrealDB.

This crate provides:
- Name resolution and scopes (`QueryScope`, `ScopeStack`).
- Dialect policies (`DialectPolicy`, `NamePolicy`, `NullOrderPolicy`).
- Unified bound IR (`BoundSelect`, `BoundExpr`, `BoundTableRef`, `BoundTarget`, etc.).
- The binder skeleton for PostgreSQL and MySQL ASTs.
- `BinderCatalog` abstraction for table schema and column resolution.
- Lowering from Bound IR to SurrealDB `LogicalOp` execution plans.
