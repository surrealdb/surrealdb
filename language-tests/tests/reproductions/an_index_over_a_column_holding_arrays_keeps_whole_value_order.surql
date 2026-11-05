/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
ORDER BY over a plain indexed column with no declared kind sorts whole values,
whichever access path the scan takes. Such a column stores a record holding an
array once per element, so the index's key order is element order: t:1 sits at
'ant' and at 'bat', before every string, while its whole value, an array,
sorts after every string.
An index scan over that column delivers no ordering of the column, and the
sort above it must stay. Every indexed read answers what its WITH NOINDEX twin
answers."""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, tags: ['ant', 'bat'] }]"

[[test.results]]
value = "[{ id: t:2, tags: 'bee' }]"

[[test.results]]
value = "[{ id: t:3, tags: 'cat' }]"

[[test.results]]
value = "[{ id: t:2 }, { id: t:3 }, { id: t:1 }]"

[[test.results]]
value = "[{ id: t:2 }, { id: t:3 }, { id: t:1 }]"

[[test.results]]
value = "[{ id: t:2 }, { id: t:3 }]"

[[test.results]]
value = "[{ id: t:2 }, { id: t:3 }]"

[[test.results]]
value = "[{ id: t:1 }, { id: t:3 }, { id: t:2 }]"

[[test.results]]
value = "[{ id: t:1 }, { id: t:3 }, { id: t:2 }]"

[[test.results]]
value = "[{ id: t:2 }, { id: t:3 }, { id: t:1 }]"

[[test.results]]
value = "[{ id: t:2 }, { id: t:3 }, { id: t:1 }]"

*/

DEFINE TABLE t SCHEMALESS;
DEFINE INDEX t_tags ON t FIELDS tags;
-- t:1's entries both sort before 'bee', in either direction of the scan;
-- its whole value sorts after every string.
CREATE t:1 SET tags = ['ant', 'bat'];
CREATE t:2 SET tags = 'bee';
CREATE t:3 SET tags = 'cat';

-- A range the index serves, ordered by the indexed column.
SELECT id FROM t WHERE tags > 'a' ORDER BY tags;
SELECT id FROM t WITH NOINDEX WHERE tags > 'a' ORDER BY tags;

-- The same with a LIMIT, which a claimed order would push into the scan.
SELECT id FROM t WHERE tags > 'a' ORDER BY tags LIMIT 2;
SELECT id FROM t WITH NOINDEX WHERE tags > 'a' ORDER BY tags LIMIT 2;

-- Descending.
SELECT id FROM t WHERE tags > 'a' ORDER BY tags DESC;
SELECT id FROM t WITH NOINDEX WHERE tags > 'a' ORDER BY tags DESC;

-- No predicate: an order alone the index could be chosen for.
SELECT id FROM t ORDER BY tags;
SELECT id FROM t WITH NOINDEX ORDER BY tags;
