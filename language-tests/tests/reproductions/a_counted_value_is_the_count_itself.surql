/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	Counting by key — and counting index entries — answers
	`SELECT count() … GROUP ALL` without reading a record, and the scan that
	does it builds the answering row itself, standing in for the aggregation
	*and* the projection above it. `SELECT VALUE count()` asks for the count
	rather than for a row holding it, and writes an alias that names nothing;
	the projection the shortcut replaced is what used to say so.

	A shortcut decides which plan runs, never what the plan answers, so every
	statement here has to read the same whether or not a scan answered it.
	Each of the shortcut's own emit sites is reached: a whole table, a
	record-id range, a `COUNT` index whose guard is the predicate, a B-tree
	walk, and a fused bitmap over two indexes.

	The controls bound it on both sides: a source that is no key range takes no
	shortcut and must still agree, and the same counts without `VALUE` keep the
	row — and the alias's key in it — that `VALUE` is asking to be rid of.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

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
value = "[3]"

[[test.results]]
value = "[3]"

[[test.results]]
value = "[3]"

[[test.results]]
value = "3"

[[test.results]]
value = "[3]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[2]"

[[test.results]]
value = "[2]"

[[test.results]]
value = "[2]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[2]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ n: 3 }]"

*/

DEFINE TABLE e SCHEMALESS;
DEFINE FIELD a ON e TYPE int;
DEFINE FIELD b ON e TYPE int;
DEFINE INDEX e_a ON e FIELDS a;
DEFINE INDEX e_b ON e FIELDS b;
DEFINE INDEX e_a_one ON e COUNT WHERE a = 1;
CREATE e:1 SET a = 1, b = 2 RETURN NONE;
CREATE e:2 SET a = 2, b = 2 RETURN NONE;
CREATE e:3 SET a = 1, b = 3 RETURN NONE;

-- Counted by key: the count is the answer, not a row carrying it.
SELECT VALUE count() FROM e GROUP ALL;

-- A predicate that folds away leaves the same plan.
SELECT VALUE count() FROM e WHERE true GROUP ALL;

-- An alias has no output key of its own under VALUE.
SELECT VALUE count() AS n FROM e GROUP ALL;

-- The tail the caller wraps around the scan reads the counted value too.
SELECT VALUE count() FROM ONLY e GROUP ALL;
SELECT VALUE count() FROM e GROUP ALL LIMIT 1;
SELECT VALUE count() FROM e GROUP ALL START 1;

-- A record-id range is counted by key as a table is.
SELECT VALUE count() FROM e:1..=2 GROUP ALL;

-- A COUNT index whose guard is the predicate answers from its deltas.
SELECT VALUE count() FROM e WHERE a = 1 GROUP ALL;
SELECT VALUE count() AS n FROM e WHERE a = 1 GROUP ALL;

-- No COUNT index covers these: a B-tree walk, then a fused bitmap.
SELECT VALUE count() FROM e WHERE a = 2 GROUP ALL;
SELECT VALUE count() FROM e WHERE a = 1 AND b = 2 GROUP ALL;

-- Control: an array source is no key range, so nothing is counted by key.
SELECT VALUE count() FROM [{}, {}] GROUP ALL;

-- Control: without VALUE the row is the answer, and an alias is its key.
SELECT count() FROM e GROUP ALL;
SELECT count() AS n FROM e GROUP ALL;
