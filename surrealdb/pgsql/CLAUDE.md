# Instructions for `surrealdb-pgsql`

This crate implements the PostgreSQL and Standard SQL language front end for SurrealDB.

## Core Rules & Architecture

1. **Grammar Authority**:
   - PostgreSQL `gram.y`, `scan.l`, and `kwlist.h` are the primary authorities for grammar, tokenization, keywords, and rejections.
   - All parser functions must cite the gram.y nonterminal they implement (e.g. `/// gram.y: a_expr`).

2. **Test Oracle & pg_query**:
   - `pg_query` serves as a test oracle and node-model reference only.
   - `pg_query` is NEVER linked into this crate or any main-repo crate. Conformance data is precomputed in the external conformance repository and verified via structural comparisons.

3. **Provenance & Reference Sources**:
   - See `PROVENANCE.md`.
   - Never use or import banned sources: MySQL GPL sources (`sql_yacc.yy`), Oracle ANTLR grammars, MySQL manual text/examples, or ISO BNF transcriptions.

4. **Security & Invariants**:
   - Strictly no logging or tracing (`log::`, `tracing::`) in parser crates.
   - No panic on any input (`#![forbid(unsafe_code)]` and strict clippy denies).
   - Re-printed SQL is never executed or forwarded by product code.
   - Credentials must be wrapped in `Secret<T>` and redacted by default in `Display`, `Debug`, and `AstDump`.

5. **Grammar PR Checklist**:
   - Spec accept/reject test cases included.
   - Projection arm citing the gram.y action line.
   - Feature key in `features.toml` with an enumerated source.
   - Replicated rejections and limits depth charges tested.
