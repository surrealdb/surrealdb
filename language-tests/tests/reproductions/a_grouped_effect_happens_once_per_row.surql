/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	An expression a grouping statement evaluates per row — a group key, an
	aggregate's first argument — that does something besides compute its value
	does it once per row, in the order the statement is written: for each row in
	turn, the group keys, then each aggregate's argument from left to right.

	Advancing a sequence is that something, and the sequence counts it: each
	query is followed by the next value, which says how many draws the statement
	made, and the totals say which draw reached which column. Evaluating a whole
	column before the next would draw the same values in a different order, and
	evaluating a column again after it failed part-way would draw more of them
	than the statement has rows. Both are shapes an engine may take where the
	expressions have nothing to repeat or reorder; neither may be visible in
	what a statement consumes.

	The third query raises, which is what makes an engine that evaluates a
	column at a time evaluate it a second time. The value it raises is not one
	an aggregate may skip, so both engines report it, and the draw count is the
	assertion.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: r:1, ok: true }]"

[[test.results]]
value = "[{ id: r:2, ok: true }]"

[[test.results]]
value = "[{ id: r:3, ok: false }]"

[[test.results]]
value = "[{ a: 6f, b: 309f }]"

[[test.results]]
value = "6"

[[test.results]]
value = "[{ a: 1f, k: 0 }, { a: 3f, k: 2 }, { a: 5f, k: 4 }]"

[[test.results]]
value = "6"

[[test.results]]
error = "An error occurred: nope"

[[test.results]]
value = "3"
*/

-- A batch wide enough that no draw here reserves another one; the values a
-- sequence hands out are contiguous whatever the batch is.
DEFINE SEQUENCE s BATCH 1000 START 0;
DEFINE SEQUENCE u BATCH 1000 START 0;
DEFINE SEQUENCE e BATCH 1000 START 0;
CREATE r:1 SET ok = true;
CREATE r:2 SET ok = true;
CREATE r:3 SET ok = false;

-- Two aggregates over the same sequence: each row draws for the first column
-- and then for the second.
SELECT math::sum(sequence::nextval('s') + 0) AS a, math::sum(sequence::nextval('s') + 100) AS b
FROM r GROUP ALL;
RETURN sequence::nextval('s');

-- A row's key is drawn before that row's aggregate argument.
SELECT sequence::nextval('u') AS k, math::sum(sequence::nextval('u')) AS a FROM r GROUP BY k;
RETURN sequence::nextval('u');

-- The third row raises once it has drawn, and no row draws twice: three rows,
-- three draws.
SELECT math::sum(sequence::nextval('e') + IF ok THEN 0 ELSE THROW 'nope' END) AS t
FROM r GROUP ALL;
RETURN sequence::nextval('e');
