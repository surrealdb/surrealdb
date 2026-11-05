# Architecture & Design of `surrealdb-pgsql`

## Target Architecture

1. **PG-Faithful AST**:
   - 1:1 structural correspondence with PostgreSQL's raw parse nodes (`pg_query` node vocabulary).
   - Preserves surface spellings via explicit `#[marker]` annotations.
   - Eliminates untyped DefElem lists in favor of typed option enums.
   - Typed SQL-syntax forms for standard functions (`TRIM`, `EXTRACT`, `SUBSTRING`, etc.).

2. **Parsing Stack**:
   - Zero-allocation, linear-time lexer respecting PostgreSQL's exact tokenizing rules.
   - Top-down operator precedence (Pratt parsing) for expressions, sharing precedence tables with the printer.
   - Explicit depth budgeting charged on all recursive productions.

3. **Projection & Verification**:
   - Test-only structural projection $\pi$ maps our AST to canonical `pg_query` JSON.
   - Verified against precomputed oracle trees from `surrealdb/sql-conformance`.
