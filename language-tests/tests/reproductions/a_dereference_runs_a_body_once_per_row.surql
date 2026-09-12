/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	Reading a field off a record link fetches that record, and the fetch
	derives the record's `COMPUTED` fields — bodies the projection does not
	hold and cannot see. So a dereference is not the plain snapshot read it
	looks like: what it leaves behind is whatever those bodies do, and a body
	drawing from a sequence advances the sequence once per fetch.

	`a_projected_effect_happens_once_per_row` pins the same property for an
	effect the projection can see, and the operators may evaluate a batch a
	column at a time where there is none. A dereference has to be counted among
	the effects that forbid it, or `SELECT link.n AS a, link.n AS b` reports
	which column was evaluated first instead of what the row holds.

	Each shape gets its own table and its own sequence, so the value a field
	reports is the draw that field made, and the `sequence::nextval` after each
	query counts the draws the statement made. Every shape below has two
	drawing columns, and each states a pairing only the row-major order
	produces: reordering the columns would leave the draw count alone and move
	the values between them. `SELECT VALUE` is deliberately absent — it holds
	one expression, so it has no second column to interleave with, and what its
	gate guards is a part-way retry rather than an order.

	The four shapes each choose their batch shape at a separate gate: a flat
	projection, a projection whose output paths nest and are built by the
	projection itself, a `[WHERE …]` filter in terminal position — whose
	predicate is evaluated with each element as the current value, so it
	fetches without any step in it naming the element — and a grouped
	projection, whose keys are judged together with its fields.
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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 0, b: 1 }, { a: 2, b: 3 }]"

[[test.results]]
value = "4"

[[test.results]]
value = "[{ x: { a: 0, b: 1 } }, { x: { a: 2, b: 3 } }]"

[[test.results]]
value = "4"

[[test.results]]
value = "[{ a: [pu:1], b: [] }, { a: [pu:1], b: [] }]"

[[test.results]]
value = "4"

[[test.results]]
value = "[{ a: 0, b: 1 }, { a: 2, b: 3 }]"

[[test.results]]
value = "4"
*/

-- A batch wide enough that no draw here reserves another one; the values a
-- sequence hands out are contiguous whatever the batch is.
DEFINE SEQUENCE s BATCH 1000 START 0;
DEFINE SEQUENCE t BATCH 1000 START 0;
DEFINE SEQUENCE u BATCH 1000 START 0;
DEFINE SEQUENCE v BATCH 1000 START 0;

DEFINE TABLE ps SCHEMALESS;
DEFINE FIELD n ON ps COMPUTED sequence::nextval('s');
CREATE ps:1 RETURN NONE;

DEFINE TABLE pt SCHEMALESS;
DEFINE FIELD n ON pt COMPUTED sequence::nextval('t');
CREATE pt:1 RETURN NONE;

DEFINE TABLE pu SCHEMALESS;
DEFINE FIELD n ON pu COMPUTED sequence::nextval('u');
CREATE pu:1 RETURN NONE;

DEFINE TABLE pv SCHEMALESS;
DEFINE FIELD n ON pv COMPUTED sequence::nextval('v');
CREATE pv:1 RETURN NONE;

-- Two fields dereferencing the same link: each row fetches for the first field
-- and then for the second.
SELECT link.n AS a, link.n AS b FROM [{ link: ps:1 }, { link: ps:1 }];
RETURN sequence::nextval('s');

-- A projection whose output paths nest, which the projection builds itself.
SELECT link.n AS x.a, link.n AS x.b FROM [{ link: pt:1 }, { link: pt:1 }];
RETURN sequence::nextval('t');

-- A `[WHERE …]` filter in terminal position. Its predicate reads `n` off each
-- element, so each element that is a record id is fetched — and the draw
-- decides whether the element survives, which is what makes the pairing
-- visible in the rows rather than only in the count. Row-major keeps the even
-- draw for `a` in both rows; column-major would give `a` both of the first two
-- draws and leave `b` the odd ones.
SELECT links[WHERE n % 2 = 0] AS a, links[WHERE n % 2 = 0] AS b
	FROM [{ links: [pu:1] }, { links: [pu:1] }];
RETURN sequence::nextval('u');

-- A grouped projection with two drawn keys, so the pairing decides which
-- values share a group rather than only what the groups are called.
SELECT link.n AS a, link.n AS b FROM [{ link: pv:1 }, { link: pv:1 }] GROUP BY a, b;
RETURN sequence::nextval('v');
