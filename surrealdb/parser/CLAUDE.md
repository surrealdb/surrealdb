# CLAUDE.md

## Parser Tests (`surrealdb/parser/src/test/files/*.surql`)

The parser crate has its own file-based test suite. When adding a parser test, add a `.surql`
file under `surrealdb/parser/src/test/files/` (or `files_quirk/` for quirk-mode parsing) instead
of writing a test function in `src/test/mod.rs`. These tests verify that the source parses and
that the generated AST matches the expected block embedded in the file after
`/* ===== result =====`. Sources that fail to parse are also supported: the expected block then
contains the rendered parsing error, so error messages are pinned too.

Generate or update expected output with the `RESULT` environment variable:

```bash
# Fill in expectations for files that don't have one yet
RESULT=ACCEPT cargo test -p surrealdb-parser text_test

# Rewrite expectations that have changed (review the diff before committing)
RESULT=OVERWRITE cargo test -p surrealdb-parser text_test
```

Only add a function test in `src/test/mod.rs` for uncommon parser functionality that a plain
source-file test can't express.
