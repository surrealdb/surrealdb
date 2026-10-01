/**
[test]
reason = """
A CONTAINS leaf whose value is NULL or NONE is never answered by the index key
alone. An empty array has no element to index, so the writer keys its column as
NULL, and an absent field evaluates `tags.*` to NONE and is keyed by that; an
entry carrying either therefore does not prove the column holds such an
element. The leaf stays in the residual, so `count()` cannot be answered by
counting entries under the pinned prefix. Each count is paired with its WITH
NOINDEX oracle, on a compound index (the leaf pins the trailing fanning column)
and on a single-column one (the leaf is the whole access), and CONTAINSANY is
covered because a union branch keyed NULL admits the same rows."""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, id: p:1 }]"

[[test.results]]
value = "[{ a: 1, id: p:2, tags: [] }]"

[[test.results]]
value = "[{ a: 1, id: p:3, tags: [NULL] }]"

[[test.results]]
value = "[{ a: 1, id: p:4, tags: [NONE] }]"

[[test.results]]
value = "[{ a: 1, id: p:5, tags: ['x'] }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[p:3]"

[[test.results]]
value = "[p:3]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

*/
DEFINE TABLE p SCHEMALESS;
DEFINE INDEX p_a_tags ON p FIELDS a, tags.*;
DEFINE INDEX p_tags ON p FIELDS tags.*;
CREATE p:1 SET a = 1;
CREATE p:2 SET a = 1, tags = [];
CREATE p:3 SET a = 1, tags = [NULL];
CREATE p:4 SET a = 1, tags = [NONE];
CREATE p:5 SET a = 1, tags = ['x'];

-- The trailing fanning column pinned to NULL: p:2's empty array is keyed there
-- too, and must not be counted.
SELECT count() FROM p WHERE a = 1 AND tags CONTAINS NULL GROUP ALL;
SELECT count() FROM p WITH NOINDEX WHERE a = 1 AND tags CONTAINS NULL GROUP ALL;

-- The same column pinned to NONE: p:1's absent field is keyed there too.
SELECT count() FROM p WHERE a = 1 AND tags CONTAINS NONE GROUP ALL;
SELECT count() FROM p WITH NOINDEX WHERE a = 1 AND tags CONTAINS NONE GROUP ALL;

-- A union branch keyed NULL admits p:2 the same way.
SELECT count() FROM p WHERE tags CONTAINSANY [NULL, 'x'] GROUP ALL;
SELECT count() FROM p WITH NOINDEX WHERE tags CONTAINSANY [NULL, 'x'] GROUP ALL;

-- The rows, which were right before the counts were: the row path re-checks
-- the leaf.
SELECT VALUE id FROM p WHERE tags CONTAINS NULL ORDER BY id;
SELECT VALUE id FROM p WITH NOINDEX WHERE tags CONTAINS NULL ORDER BY id;

-- The single-column index, where the leaf is the whole access.
SELECT count() FROM p WHERE tags CONTAINS NULL GROUP ALL;
SELECT count() FROM p WITH NOINDEX WHERE tags CONTAINS NULL GROUP ALL;
SELECT count() FROM p WHERE NULL INSIDE tags GROUP ALL;
SELECT count() FROM p WITH NOINDEX WHERE NULL INSIDE tags GROUP ALL;
