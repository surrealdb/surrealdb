/**
[env]
namespace = true
database = true
backend = ["rocksdb"]

[test]
wip = true
reason = """
A mutation in scalar position, over a table with more than one row, panics the
RocksDB backend: `Cursor alive while trying to rollback transaction`
(`surrealdb/kvs-rocksdb/src/owned_tx.rs`). The scan's cursor is still borrowed
when the nested write rolls back to its savepoint, so `get_mut` finds a strong
count above one and panics rather than segfaulting, which the comment there
says is deliberate until the ownership is fixed.

Three conditions are each necessary. The form must take a savepoint —
`UPSERT` and `INSERT` do, for their create-then-update retry — so a nested
`CREATE` does not reproduce it. The outer table must yield at least two rows;
with one row the cursor is finished before the write. And the backend must be
RocksDB: `mem` and `surrealkv` pass.

`wip` is set
because the expectations below record intended behaviour, not current: the run
panics, and the marker keeps that visible as a warning instead of red.
"""

[[test.results]]
value = "[{ id: person:a }]"

[[test.results]]
value = "[{ id: person:b }]"

[[test.results]]
value = "[{ bumped: [{ id: counter:x, n: 1 }], id: person:a }, { bumped: [{ id: counter:x, n: 1 }], id: person:b }]"
*/

CREATE person:a;
CREATE person:b;
SELECT id, (UPSERT counter:x SET n = 1) AS bumped FROM person;
