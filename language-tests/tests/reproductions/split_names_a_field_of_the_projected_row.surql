/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	`SPLIT` names a field of the row the statement returns — the parser
	rejects a target that is not in the selection — so the split happens after
	the projection. It sees the value the projection produced, under the name
	the projection gave it.

	That matters wherever the two rows differ: an alias names a field the
	source row does not have, a nested path may only exist once the projection
	has built it, a name the projection reuses holds a different value on
	either side, and a second field reading the split one is owed the whole of
	it rather than one element.
"""

[[test.results]]
value = "[{ id: tag:1, tags: [1, 2] }]"

[[test.results]]
value = "[{ id: t:1, link: tag:1, other: [1, 2], own: [7, 8, 7] }]"

[[test.results]]
value = "[{ id: t:1, own: 7 }, { id: t:1, own: 8 }, { id: t:1, own: 7 }]"

[[test.results]]
value = "[{ id: t:1, link: tag:1, other: [1, 2], own: 7 }, { id: t:1, link: tag:1, other: [1, 2], own: 8 }, { id: t:1, link: tag:1, other: [1, 2], own: 7 }]"

[[test.results]]
value = "[{ id: t:1, own: 7 }, { id: t:1, own: 8 }, { id: t:1, own: 7 }]"

[[test.results]]
value = "[{ id: t:1, own: 7 }, { id: t:1, own: 7 }]"

[[test.results]]
value = "[{ id: t:1, x: 7 }, { id: t:1, x: 8 }, { id: t:1, x: 7 }]"

[[test.results]]
value = "[{ id: t:1, x: 1 }, { id: t:1, x: 2 }, { id: t:1, x: 3 }]"

[[test.results]]
value = "[{ id: t:1, x: 1 }, { id: t:1, x: 2 }]"

[[test.results]]
value = "[{ id: t:1, link: { tags: 1 } }, { id: t:1, link: { tags: 2 } }]"

[[test.results]]
value = "[{ id: t:1, own: 1 }, { id: t:1, own: 2 }]"

[[test.results]]
value = "[{ id: t:1, own: 7 }, { id: t:1, own: 8 }]"

[[test.results]]
value = "[{ a: [7, 8, 7], id: t:1, own: 7 }, { a: [7, 8, 7], id: t:1, own: 8 }, { a: [7, 8, 7], id: t:1, own: 7 }]"

[[test.results]]
value = "[{ a: 7, b: 1, id: t:1 }, { a: 7, b: 2, id: t:1 }, { a: 8, b: 1, id: t:1 }, { a: 8, b: 2, id: t:1 }, { a: 7, b: 1, id: t:1 }, { a: 7, b: 2, id: t:1 }]"
*/

CREATE tag:1 SET tags = [1, 2];
CREATE t:1 SET own = [7, 8, 7], other = [1, 2], link = tag:1;

-- A field carried through under its own name splits the same either way.
SELECT id, own FROM t SPLIT own;
SELECT * FROM t SPLIT own;
SELECT id, own FROM t WHERE own CONTAINS 7 SPLIT own;
SELECT id, own FROM t SPLIT own ORDER BY own LIMIT 2;

-- An alias names a field only the projected row has.
SELECT id, own AS x FROM t SPLIT x;
SELECT id, [1, 2, 3] AS x FROM t SPLIT x;

-- A path through a record link is built by the projection; the stored row
-- holds the link itself.
SELECT id, link.tags AS x FROM t SPLIT x;
SELECT id, link.tags FROM t SPLIT link.tags;

-- A name the projection reuses holds one value before it and another after.
SELECT id, other AS own FROM t SPLIT own;
SELECT id, array::distinct(own) AS own FROM t SPLIT own;

-- A field reading the split one is owed the whole of it.
SELECT id, own AS a, own FROM t SPLIT own;

-- Two targets split the row by each in turn.
SELECT id, own AS a, other AS b FROM t SPLIT a, b;
