# Provenance & Reference Sources

## Permitted Reference Sources

1. **PostgreSQL Upstream**:
   - `src/backend/parser/gram.y`, `scan.l`, `kwlist.h`, `parser.c`, `scansup.c`
   - Branches: `REL_15_STABLE` (feature IDs), `REL_17_STABLE`, `REL_18_STABLE`, `REL_19_STABLE`
   - Reference specification (clean-room implementation; no upstream code is copied or linked)

2. **Permissive References (Read-Only)**:
   - `apache/datafusion-sqlparser-rs` (Apache-2.0): Read-only reference for Rust idioms and regression lessons.
   - `squawk` (Apache-2.0 OR MIT): Permissive linter grammar, used as positive control and reference.
   - `postgres_deparse.c` / `pgls_pretty_print`: Reference for deparsing keyword orders.

## Prohibited Sources

The following sources are banned:
- MySQL GPL grammar sources (`sql_yacc.yy`, `sql_prepare.cc`, `lex.h`)
- Oracle ANTLR MySQL grammars
- MySQL manual text, documentation examples, and syntax synopses
- ISO 9075 BNF text or third-party transcriptions
- Datasets under CC BY-SA (Spider, BIRD)
