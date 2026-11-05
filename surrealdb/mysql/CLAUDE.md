# Instructions for `surrealdb-mysql`

This crate implements the MySQL language front end for SurrealDB.

## Core Rules & Architecture

1. **Clean-Room Implementation**:
   - Zero GPL code: Never copy, reference, or import from MySQL GPL sources (`sql_yacc.yy`, `sql_prepare.cc`, `lex.h`).
   - Clean-room specification and client-side wire PREPARE oracles are used for validation.

2. **Provenance & Reference Sources**:
   - See `PROVENANCE.md`.
   - Never use or import banned sources: MySQL GPL sources (`sql_yacc.yy`), Oracle ANTLR grammars, MySQL manual text/examples, or ISO BNF transcriptions.

3. **Security & Invariants**:
   - Strictly no logging or tracing (`log::`, `tracing::`) in parser crates.
   - No panic on any input (`#![forbid(unsafe_code)]` and strict clippy denies).
   - Re-printed SQL is never executed or forwarded by product code.
   - Credentials must be wrapped in `Secret<T>` and redacted by default in `Display`, `Debug`, and `AstDump`.
