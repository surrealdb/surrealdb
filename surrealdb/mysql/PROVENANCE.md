# Provenance & Reference Sources for `surrealdb-mysql`

## Clean-Room Implementation Architecture

This crate is a clean-room implementation of the MySQL SQL dialect.
Zero code, headers, or internal grammar structures are derived or copied from MySQL or Oracle sources.

## Permitted Reference Sources (Read-Only)

1. **Permissive References**:
   - `apache/datafusion-sqlparser-rs` (Apache-2.0): Read-only reference for general SQL dialect differences.
   - Client-side MySQL test oracles (via external test harness testing wire-level prepare/execute responses).

## Prohibited Sources (STRICTLY BANNED)

The following sources are banned:
- MySQL GPL grammar sources (`sql_yacc.yy`, `sql_prepare.cc`, `lex.h`, or any MySQL Server C++ sources)
- Oracle ANTLR MySQL grammars
- MySQL manual text, documentation examples, and syntax synopses
- ISO 9075 BNF text or third-party transcriptions
- Datasets under CC BY-SA (Spider, BIRD)
