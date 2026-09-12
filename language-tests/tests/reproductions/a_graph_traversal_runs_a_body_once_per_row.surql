/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A graph traversal reads records: the edges of its base, and the records
	those edges name. What it yields of them is the materialised record —
	`COMPUTED` fields derived, `PERMISSIONS` judged — so two traversals of one
	base answer from two separate materialisations, and a body drawing from a
	sequence advances the sequence once per traversal.

	That makes a traversal an effect, in the same sense a record dereference
	is one, and the operators may not evaluate a batch a column at a time
	where one appears. `a_dereference_runs_a_body_once_per_row` pins the same
	property for a field read off a link; this file pins it for the hop, which
	the classifier reached through a different node and for a while called
	effect-free.

	Each shape has two traversals of the same base and its own sequence, so
	the value a field reports is the draw that field made and the
	`sequence::nextval` after each query counts the draws the statement made.
	Both state a pairing only the row-major order produces: reordering the
	columns would leave the draw count alone and move the values between them.
	Column-major would give `a` the first two draws and `b` the last two.

	The draw after the `RELATE`s is the baseline that makes the rest legible:
	creating an edge does not derive its `COMPUTED` fields, so every draw
	counted afterwards belongs to a traversal.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

# Neither RELATE drew.
[[test.results]]
value = "0"

[[test.results]]
value = "0"

[[test.results]]
value = "[{ a: [{ n: 1 }], b: [{ n: 2 }] }, { a: [{ n: 3 }], b: [{ n: 4 }] }]"

[[test.results]]
value = "5"

[[test.results]]
value = "[{ x: { a: [{ n: 1 }], b: [{ n: 2 }] } }, { x: { a: [{ n: 3 }], b: [{ n: 4 }] } }]"

[[test.results]]
value = "5"
*/

-- A batch wide enough that no draw here reserves another one.
DEFINE SEQUENCE s BATCH 1000 START 0;
DEFINE SEQUENCE t BATCH 1000 START 0;

DEFINE TABLE person SCHEMALESS;
CREATE person:a, person:b, person:c RETURN NONE;

DEFINE TABLE knows SCHEMALESS TYPE RELATION;
DEFINE FIELD n ON knows COMPUTED sequence::nextval('s');
RELATE person:a->knows->person:b RETURN NONE;
RELATE person:b->knows->person:c RETURN NONE;

DEFINE TABLE likes SCHEMALESS TYPE RELATION;
DEFINE FIELD n ON likes COMPUTED sequence::nextval('t');
RELATE person:a->likes->person:b RETURN NONE;
RELATE person:b->likes->person:c RETURN NONE;

-- The baseline: creating an edge derives none of its computed fields.
RETURN sequence::nextval('s');
RETURN sequence::nextval('t');

-- Two traversals of the same base, each row materialising its edge twice.
SELECT ->(SELECT n FROM knows) AS a, ->(SELECT n FROM knows) AS b FROM [person:a, person:b];
RETURN sequence::nextval('s');

-- The same into nested output paths, which the projection builds itself.
SELECT ->(SELECT n FROM likes) AS x.a, ->(SELECT n FROM likes) AS x.b FROM [person:a, person:b];
RETURN sequence::nextval('t');
