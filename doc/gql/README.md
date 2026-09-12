# Vendored ISO GQL grammar (opengql)

This directory vendors the ANTLR4 grammar for ISO GQL (ISO/IEC 39075:2024,
"Graph Query Language") maintained by the opengql project, together with a
distilled implementation reference used by the hand-written parser in
`surrealdb/core/src/gql/`.

**Normative reference only — never a build-time codegen input.** The SurrealDB
GQL parser is written by hand, mirroring `surrealdb/core/src/syn/` conventions.
Nothing in the build (or any `build.rs`) reads these files; they exist so that
reviewers and future CI conformance oracles can check the hand-written parser
against the same grammar text.

## Provenance

| | |
|---|---|
| Source repository | <https://github.com/opengql/grammar> |
| File | [`GQL.g4`](./GQL.g4) (vendored verbatim) |
| Branch / commit | `main` @ `16ea71bd320ad07fd2c46a3066afbaef7d226922` (committed 2025-06-17) |
| Upstream grammar version | 1.9.0 (per upstream `version.json`) |
| Fetched | 2026-06-11 |
| Integrity | `git hash-object GQL.g4` = `d857ef9b6eac10ff723470de0bd8029c9d195d63`, matching the blob SHA reported by the GitHub contents API for that commit |

## License

The upstream repository's `LICENSE` file is vendored verbatim alongside the
grammar ([`LICENSE`](./LICENSE)). It is the standard **Apache License,
Version 2.0** text; the file begins:

> Apache License
> Version 2.0, January 2004
> http://www.apache.org/licenses/

`GQL.g4` and `LICENSE` are unmodified copies and remain under that license and
upstream copyright. The distilled notes in [`REFERENCE.md`](./REFERENCE.md) and
the lowering contract in [`LOWERING.md`](./LOWERING.md) are original SurrealDB
documentation; the former is derived from reading the grammar.

## Files

- `GQL.g4` — the full ANTLR4 grammar, verbatim.
- `LICENSE` — upstream Apache-2.0 license text, verbatim.
- `README.md` — this file (provenance + license).
- `REFERENCE.md` — distilled, implementation-ready reference for the supported
  read-only GQL surface (MATCH / OPTIONAL MATCH / multi-clause + multi-pattern /
  variable-length quantifiers / path variables / WHERE / RETURN [DISTINCT] /
  ORDER BY / SKIP-OFFSET / LIMIT), with the grammar production names each section
  derives from and a v1 -> v2 behaviour-change table (§(h)).
- `LOWERING.md` — the normative contract for lowering the parsed GQL AST onto
  the `MatchPlan` IR (`surrealdb/core/src/expr/match_plan.rs`), carried by the
  logical plan as `LogicalOp::Match` and executed by the streaming engine; implemented
  by `surrealdb/core/src/gql/lower/`.

## Updating

Re-fetch `GQL.g4` and `LICENSE` from upstream `main`, update the commit hash,
date, and blob SHA in the table above, and re-verify every section of
`REFERENCE.md` against the new grammar text before changing the parser.
