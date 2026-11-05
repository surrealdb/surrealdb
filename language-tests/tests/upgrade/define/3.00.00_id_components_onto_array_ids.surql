/**
[env]
imports = ["upgrade/define/3.00.00_id_components_onto_array_ids_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
version = ">=3.4.0-a"
reason = """
Record ids an older release wrote as plain arrays can be given named components
by this checkout, and every read by a component runs over keys that release laid
down. `DEFINE FIELD ... TYPE [city: string, time: datetime]` reads every record
the table holds, so its scan decodes the older writer's keys and bodies. The
reads that follow are served from those same keys, which no write of this
checkout produced. A datetime in a record key is its RFC 3339 text, so a key
with a sub-second part (`...:00.500Z`) sorts before the whole second it falls
in, and a range that ignored this would lose the row beside a bound that a table
scan returns. Ordinary language tests write their keys with the binary that
reads them, so a disagreement between the two encodings is invisible to them.

The syntax is this checkout's. No released binary parses a named record id, so
the body runs only where the upgrade target is this checkout: the seed axis,
each old release straight to HEAD, and the final adjacent hop. The
release-to-release hops of the pair axis cannot reach the construct until a
release ships the syntax, and neither can the cumulative walk, which needs a
release to write the structure and carry it through every hop. The import writes
plain arrays, which every release writes, so `introduced` stays 3.0.0.

Plan assertions and writes are in 3.00.00_id_components_onto_array_ids_current.surql."""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[10, 11, 12, 13, 20, 21]"

[[test.results]]
value = "[10, 11, 12, 13]"

[[test.results]]
value = "[10, 11]"

[[test.results]]
value = "[21]"

[[test.results]]
value = "[13, 12, 11, 10]"

[[test.results]]
value = "[{ city: 'Paris', id: reading:['Paris', d'2025-01-01T10:00:00Z'], temp: 20, time: d'2025-01-01T10:00:00Z' }, { city: 'Paris', id: reading:['Paris', d'2025-01-01T10:00:00.250Z'], temp: 21, time: d'2025-01-01T10:00:00.250Z' }]"

[[test.results]]
value = "[{ city: 'London', id: reading:['London', d'2025-01-01T10:00:00.500Z'], temp: 11, time: d'2025-01-01T10:00:00.500Z' }]"

[[test.results]]
value = "[10, 11, 12]"

[[test.results]]
value = "[10, 11]"

*/
-- TODO(surrealdb/planning#TBD): once a release ships the named record id, add the
-- writer pair upgrade/define/<release>_id_components_named.surql and its _import,
-- prefixed and gated with that release. Its import defines the named id on that
-- release and writes keyed rows, and this checkout reads them. Until then no axis
-- reaches a named id written by a release; this pair covers the plain arrays
-- every release writes.

-- Name the components of the ids the old binary wrote. The statement reads every
-- record first, and passes only keys and bodies the structure admits.
DEFINE FIELD OVERWRITE id ON reading TYPE [city: string, time: datetime];
-- Every row the old binary wrote is still there.
SELECT VALUE temp FROM reading ORDER BY temp;
-- A lower bound on a whole second reaches the row written with a fraction of
-- that second, whose key sorts before the bound.
SELECT VALUE temp FROM reading WHERE city = 'London' AND time >= d'2025-01-01T10:00:00Z' ORDER BY temp;
-- A window closing on the next second keeps the fraction and drops the second itself.
SELECT VALUE temp FROM reading WHERE city = 'London' AND time >= d'2025-01-01T10:00:00Z' AND time < d'2025-01-01T10:00:01Z' ORDER BY temp;
-- A strict bound on a whole second keeps the row written with a fraction of it.
SELECT VALUE temp FROM reading WHERE city = 'Paris' AND time > d'2025-01-01T10:00:00Z' ORDER BY temp;
-- The order of a datetime component is the order of the instants: the fraction
-- falls after its whole second though its key sorts before it.
SELECT VALUE temp FROM reading WHERE city = 'London' ORDER BY time DESC;
-- `*` carries the components, whether the row stored a copy of one or none.
SELECT * FROM reading WHERE city = 'Paris' ORDER BY temp;
-- A record whose key carries a fraction is found by that key.
SELECT * FROM reading:['London', d'2025-01-01T10:00:00.500Z'];
-- A record range reads the records between its bounds as values, the fraction included.
SELECT VALUE temp FROM reading:['London', d'2025-01-01T10:00:00Z']..=['London', d'2025-01-01T10:00:01Z'] ORDER BY temp;
SELECT VALUE temp FROM reading:['London', d'2025-01-01T10:00:00Z']..['London', d'2025-01-01T10:00:01Z'] ORDER BY temp;
