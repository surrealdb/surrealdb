/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
`v IN [[1, 2]]` holds for a record whose v is the whole array [1, 2], so an
indexed read finds it. A plain indexed column stores that record under its
elements, 1 and 2, and never under the array itself, so an array element of
an IN list is no key to seek: the index can answer the list's scalar elements
only, and the array elements need the records compared whole. Every indexed
read answers what its WITH NOINDEX twin answers."""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, v: [1, 2] }]"

[[test.results]]
value = "[{ id: t:2, v: 3 }]"

[[test.results]]
value = "[{ id: t:3, v: 1 }]"

[[test.results]]
value = "[t:1]"

[[test.results]]
value = "[t:1]"

[[test.results]]
value = "[t:1, t:2]"

[[test.results]]
value = "[t:1, t:2]"

[[test.results]]
value = "[t:3]"

[[test.results]]
value = "[t:3]"

*/

DEFINE TABLE t SCHEMALESS;
DEFINE INDEX t_v ON t FIELDS v;
CREATE t:1 SET v = [1, 2];
CREATE t:2 SET v = 3;
-- 1 is an element of t:1's array, not the array: no `IN [[1, 2]]` matches it.
CREATE t:3 SET v = 1;

-- A one-element list holding an array.
array::sort(SELECT VALUE id FROM t WHERE v IN [[1, 2]]);
array::sort(SELECT VALUE id FROM t WITH NOINDEX WHERE v IN [[1, 2]]);

-- A list mixing an array and a scalar.
array::sort(SELECT VALUE id FROM t WHERE v IN [[1, 2], 3]);
array::sort(SELECT VALUE id FROM t WITH NOINDEX WHERE v IN [[1, 2], 3]);

-- A scalar list still matches scalars only.
array::sort(SELECT VALUE id FROM t WHERE v IN [1]);
array::sort(SELECT VALUE id FROM t WITH NOINDEX WHERE v IN [1]);
