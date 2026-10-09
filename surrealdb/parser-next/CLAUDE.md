# CLAUDE.md

## Parser tests

The parser is pinned by a golden corpus check and by invariant harnesses,
rather than by snapshot files. A new parser test is an input added to a
corpus:

- `language-tests/tests/**/*.surql`: every statement, every suffix of it that
  starts at a token, and one mutant of each, through every entry point.
- `tests/corpus/` and `tests/corpus_quirk/`: whole scripts that exercise
  grammar corners, including inputs that must be rejected.

```bash
# Every entry point's tree or rendered error, digested per corpus file
cargo run --locked --release -p surrealdb-expr --example golden
# After an intended change, record the new digests (review the inputs first)
cargo run --locked --release -p surrealdb-expr --example golden -- --show FILE
cargo run --locked --release -p surrealdb-expr --example golden -- --bless
# One input through every entry point
cargo run --locked --release -p surrealdb-expr --example golden -- --check 'SELECT * FROM'

# Streaming, round trips, depth ceilings and tree invariants under mutation
cargo run --locked --release -p surrealdb-expr --example invariants -- --stream
cargo run --locked --release -p surrealdb-expr --example invariants -- --round-trip
cargo run --locked --release -p surrealdb-expr --example invariants -- --depth-probe
cargo run --locked --release -p surrealdb-expr --example invariants -- --fuzz
```

`cargo make ci-parser` runs all of the above. A changed digest means the
parser's output changed on some input drawn from that file: confirm the change
is intended (error wording is a product contract) before blessing.

Unit tests next to the code cover what the harnesses cannot: depth budgets,
recovery, arena reuse and the settings' budget accounting.
