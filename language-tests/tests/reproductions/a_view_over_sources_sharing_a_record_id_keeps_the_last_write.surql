/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A view's row is stored under its source record's id, so a view over two tables
whose records share an id holds one row for both. Defining the view and
maintaining it agree on that row: the source read last writes it. Defining it
reads the tables in `FROM` order, so the row is the later table's, and a write
to either source afterwards replaces it.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: dup:1, v: 2 }, { id: dup:2, v: 3 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: dup:1, v: 10 }, { id: dup:2, v: 3 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: dup:1, v: 20 }, { id: dup:2, v: 3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: dup:1, v: 10 }, { id: dup:2, v: 3 }]"
*/

DEFINE TABLE qa SCHEMALESS;
DEFINE TABLE qb SCHEMALESS;
CREATE qa:1 SET v = 1 RETURN NONE;
CREATE qa:2 SET v = 3 RETURN NONE;
CREATE qb:1 SET v = 2 RETURN NONE;
DEFINE TABLE dup AS SELECT v FROM qa, qb;
SELECT * FROM dup ORDER BY id;
UPDATE qa:1 SET v = 10 RETURN NONE;
SELECT * FROM dup ORDER BY id;
UPDATE qb:1 SET v = 20 RETURN NONE;
SELECT * FROM dup ORDER BY id;
-- The other order of the same two tables holds the other table's row.
DEFINE TABLE OVERWRITE dup AS SELECT v FROM qb, qa;
SELECT * FROM dup ORDER BY id;
