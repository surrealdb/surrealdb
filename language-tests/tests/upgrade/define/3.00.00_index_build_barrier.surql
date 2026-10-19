/**
[env]
imports = ["upgrade/define/3.00.00_index_build_barrier_import.surql"]

[test]
upgrade = true
reason = """
A release with the write barrier counts every write transaction it opens, makes
each build wait for the ones older than it, and records with a build's state
the proof that it waited (`!bw`). An index an older release built carries none
of that: its build state has no proof, and the store has no barrier key. The
reader must keep such an index answering and maintained, and must be able to
build a new index over rows only the old release wrote, waiting at a barrier no
node has raised before.

The first read lands on the old index's entries; the writes maintain it through
the current write path, and the second read shows their effect; the UNIQUE
probe collides only with an entry the old binary wrote; the new index is built
by the reader, over the old rows, and must name every one of them."""

[[test.results]]
value = "[{ id: ib:1 }, { id: ib:2 }, { id: ib:4 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: ib:4 }, { id: ib:7 }]"

[[test.results]]
error = true

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: ib:1 }, { id: ib:3 }, { id: ib:5 }, { id: ib:7 }]"

*/
-- The index the old release built answers through its entries.
SELECT id FROM ib WITH INDEX ib_f WHERE f = 'a' ORDER BY id;
-- Writes on the reader maintain it: one moves a record out of `a`, one removes
-- a record, one adds a record to `a`.
UPDATE ib:1 SET f = 'b' RETURN NONE;
DELETE ib:2 RETURN NONE;
CREATE ib:7 SET f = 'a', u = 7, g = 'x' RETURN NONE;
SELECT id FROM ib WITH INDEX ib_f WHERE f = 'a' ORDER BY id;
-- The unique index refuses a value an old row holds.
CREATE ib:8 SET u = 3;
-- A build on the reader, over rows the old release wrote, waits at the barrier
-- and indexes every one of them.
DEFINE INDEX ib_g ON TABLE ib COLUMNS g;
SELECT id FROM ib WITH INDEX ib_g WHERE g = 'x' ORDER BY id;
