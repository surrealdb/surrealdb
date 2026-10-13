/**
[env]
namespace = true
database = true
auth = { level = "owner" }
planner-strategy = ["all-ro"]

[test]
reason = "A `UnionIndexScan` may only drop a `CONTAINSANY`/`ANYINSIDE` leaf that every branch implies, i.e. one whose literals contain all the union's branch values. The union can be built from a *different* AND conjunct — a nested OR (`try_and_nested_or_union`) or another containment leaf — whose branch values reach outside a sibling leaf's literals; dropping that sibling then admits rows failing it, weakening a WHERE clause that may be carrying access scoping. Each indexed query here must agree with its `WITH NOINDEX` twin, and the EXPLAINs pin which leaves survive in the residual filter."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: item:has_a, tags: ['a'] }]"

[[test.results]]
value = "[{ id: item:has_b, tags: ['b'] }]"

[[test.results]]
value = "[{ id: item:has_ab, tags: ['a', 'b'] }]"

[[test.results]]
value = "[{ id: item:has_c, tags: ['c'] }]"

[[test.results]]
value = "[item:has_a, item:has_ab]"

[[test.results]]
value = "[item:has_a, item:has_ab]"

[[test.results]]
value = '''"ProjectValue [ctx: Db] [expr: id]\n    Filter [ctx: Db] [predicate: tags CONTAINSANY ['a'] AND tags CONTAINS 'a' OR tags CONTAINS 'b']\n        BitmapResolve [ctx: Db] [table: item]\n            BitmapAnd [ctx: Db]\n                BitmapOr [ctx: Db]\n                    BitmapIndexScan [ctx: Db] [index: ix_tags, access: = 'a']\n                BitmapOr [ctx: Db]\n                    BitmapIndexScan [ctx: Db] [index: ix_tags, access: = 'a']\n                    BitmapIndexScan [ctx: Db] [index: ix_tags, access: = 'b']\n"'''

[[test.results]]
value = "[item:has_a, item:has_ab]"

[[test.results]]
value = "[item:has_a, item:has_ab]"

[[test.results]]
value = '''"ProjectValue [ctx: Db] [expr: id]\n    Limit [ctx: Db] [limit: 10]\n        Filter [ctx: Db] [predicate: tags CONTAINSANY ['a'] AND tags CONTAINS 'a' OR tags CONTAINS 'b']\n            UnionIndexScan [ctx: Db] [table: item, branches: 2]\n                IndexScan [ctx: Db] [index: ix_tags, access: = 'a', direction: Forward]\n                IndexScan [ctx: Db] [index: ix_tags, access: = 'b', direction: Forward]\n"'''

[[test.results]]
value = "[item:has_a, item:has_ab]"

[[test.results]]
value = "[item:has_a, item:has_ab]"

[[test.results]]
value = '''"ProjectValue [ctx: Db] [expr: id]\n    Limit [ctx: Db] [limit: 10]\n        Filter [ctx: Db] [predicate: tags CONTAINSANY ['a']]\n            UnionIndexScan [ctx: Db] [table: item, branches: 2]\n                IndexScan [ctx: Db] [index: ix_tags, access: = 'a', direction: Forward]\n                IndexScan [ctx: Db] [index: ix_tags, access: = 'b', direction: Forward]\n"'''

[[test.results]]
value = "[item:has_a, item:has_ab, item:has_b]"

[[test.results]]
value = "[item:has_a, item:has_ab, item:has_b]"

[[test.results]]
value = '''"ProjectValue [ctx: Db] [expr: id]\n    Limit [ctx: Db] [limit: 10]\n        Filter [ctx: Db] [predicate: tags CONTAINS 'a' OR tags CONTAINS 'b']\n            UnionIndexScan [ctx: Db] [table: item, branches: 2]\n                IndexScan [ctx: Db] [index: ix_tags, access: = 'a', direction: Forward]\n                IndexScan [ctx: Db] [index: ix_tags, access: = 'b', direction: Forward]\n"'''
*/

DEFINE INDEX ix_tags ON item FIELDS tags[*];

CREATE item:has_a  SET tags = ['a'];
CREATE item:has_b  SET tags = ['b'];
CREATE item:has_ab SET tags = ['a', 'b'];
CREATE item:has_c  SET tags = ['c'];

-- (A) Nested-OR union with a sibling CONTAINSANY leaf. Bitmap fusion wins the
-- plan here and keeps the whole WHERE as its residual filter, so this shape is
-- correct via a different route; kept as the baseline for (B).
SELECT VALUE id FROM item WHERE tags CONTAINSANY ['a'] AND (tags CONTAINS 'a' OR tags CONTAINS 'b');
SELECT VALUE id FROM item WITH NOINDEX WHERE tags CONTAINSANY ['a'] AND (tags CONTAINS 'a' OR tags CONTAINS 'b');
EXPLAIN SELECT VALUE id FROM item WHERE tags CONTAINSANY ['a'] AND (tags CONTAINS 'a' OR tags CONTAINS 'b');

-- (B) The same WHERE with a LIMIT and no ORDER BY: bitmap fusion is skipped
-- (a streaming plan terminates early) and the nested-OR union drives the scan.
-- Its branch values are 'a' and 'b', so the sibling `CONTAINSANY ['a']` is NOT
-- implied and must stay in the residual filter — otherwise item:has_b, which
-- holds no 'a', is returned.
SELECT VALUE id FROM item WHERE tags CONTAINSANY ['a'] AND (tags CONTAINS 'a' OR tags CONTAINS 'b') LIMIT 10;
SELECT VALUE id FROM item WITH NOINDEX WHERE tags CONTAINSANY ['a'] AND (tags CONTAINS 'a' OR tags CONTAINS 'b') LIMIT 10;
EXPLAIN SELECT VALUE id FROM item WHERE tags CONTAINSANY ['a'] AND (tags CONTAINS 'a' OR tags CONTAINS 'b') LIMIT 10;

-- (C) Two containment siblings. The union is expanded from the wider leaf, so
-- that leaf is implied and drops out, while the narrower `CONTAINSANY ['a']`
-- must survive.
SELECT VALUE id FROM item WHERE tags CONTAINSANY ['a', 'b'] AND tags CONTAINSANY ['a'] LIMIT 10;
SELECT VALUE id FROM item WITH NOINDEX WHERE tags CONTAINSANY ['a', 'b'] AND tags CONTAINSANY ['a'] LIMIT 10;
EXPLAIN SELECT VALUE id FROM item WHERE tags CONTAINSANY ['a', 'b'] AND tags CONTAINSANY ['a'] LIMIT 10;

-- (D) A leaf WIDER than the branch values is still implied by every branch, so
-- the optimisation must keep firing and drop it. Only the OR conjunct that
-- built the union is left to re-check.
SELECT VALUE id FROM item WHERE tags CONTAINSANY ['a', 'b', 'z'] AND (tags CONTAINS 'a' OR tags CONTAINS 'b') LIMIT 10;
SELECT VALUE id FROM item WITH NOINDEX WHERE tags CONTAINSANY ['a', 'b', 'z'] AND (tags CONTAINS 'a' OR tags CONTAINS 'b') LIMIT 10;
EXPLAIN SELECT VALUE id FROM item WHERE tags CONTAINSANY ['a', 'b', 'z'] AND (tags CONTAINS 'a' OR tags CONTAINS 'b') LIMIT 10;
