/**
[env]
backend = ["surrealkv"]
versioned = true
timeout = 15000

[test]
reason = """
	Every shape of read an index could answer must read the records at the
	stamp, wherever the statement is planned: a stamp only the run can evaluate,
	a table named at run time, a subquery, a function body, a subquery joined
	on an indexed column, array-element indexes, a grouping, several sources and
	a pinned index. Each is asked beside its `WITH NOINDEX` twin where there is
	one, over data that changes after the stamp so that an index entry would give
	a different answer.
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
value = "NONE"

[[test.results]]
value = "[{ id: item:1, kind: 'a', rank: 1, tags: ['x', 'y'] }]"

[[test.results]]
value = "[{ id: item:2, kind: 'a', rank: 2, tags: ['y'] }]"

[[test.results]]
value = "[{ id: item:3, kind: 'b', rank: 3, tags: ['z'] }]"

[[test.results]]
value = "[{ id: anchor:0, k: 'a' }]"

[[test.results]]
value = "[{ id: anchor:1, k: 'b' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: item:4, kind: 'a', rank: 4, tags: ['x'] }]"

[[test.results]]
value = "[{ id: item:1, kind: 'b', rank: 9, tags: ['z'] }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[item:1, item:2]"

[[test.results]]
value = "[item:1, item:2]"

[[test.results]]
value = "[[item:1, item:2]]"

[[test.results]]
value = "[[item:1, item:2]]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[item:1]"

[[test.results]]
value = "[item:1]"

[[test.results]]
value = "[item:1, item:3]"

[[test.results]]
value = "[item:1, item:3]"

[[test.results]]
value = "[2, 3]"

[[test.results]]
value = "[2, 3]"

[[test.results]]
value = "[{ kind: 'a', n: 2 }, { kind: 'b', n: 1 }]"

[[test.results]]
value = "[{ kind: 'a', n: 2 }, { kind: 'b', n: 1 }]"

[[test.results]]
value = "[anchor:0, item:1, item:2]"

[[test.results]]
value = "[anchor:0, item:1, item:2]"

[[test.results]]
value = "[item:1, item:2]"

[[test.results]]
value = '''
"VersionScope [ctx: Db] [version: $ts]
    ProjectValue [ctx: Db] [expr: id]
        TableScan [ctx: Db] [table: item, direction: Forward, predicate: kind = 'a', pre_decode_filter: yes]
"'''
*/

DEFINE TABLE item SCHEMALESS;
DEFINE TABLE anchor SCHEMALESS;
DEFINE INDEX item_kind ON item FIELDS kind;
DEFINE INDEX item_rank ON item FIELDS rank;
DEFINE INDEX item_tags ON item FIELDS tags.*;
DEFINE FUNCTION fn::kind_a() { RETURN (SELECT VALUE id FROM item WHERE kind = "a" ORDER BY id) };
DEFINE FUNCTION fn::table_name() { RETURN "item" };
CREATE item:1 SET kind = "a", rank = 1, tags = ["x", "y"];
CREATE item:2 SET kind = "a", rank = 2, tags = ["y"];
CREATE item:3 SET kind = "b", rank = 3, tags = ["z"];
CREATE anchor:0 SET k = "a";
CREATE anchor:1 SET k = "b";
SLEEP 50ms;
LET $ts = time::now();
SLEEP 50ms;
CREATE item:4 SET kind = "a", rank = 4, tags = ["x"];
UPDATE item:1 SET kind = "b", rank = 9, tags = ["z"];
DELETE item:2;

-- A stamp only the run can evaluate.
SELECT VALUE id FROM item WHERE kind = "a" ORDER BY id VERSION $ts + 0ns;
-- A table named at run time.
SELECT VALUE id FROM type::table(fn::table_name()) WHERE kind = "a" ORDER BY id VERSION $ts;
-- A subquery and a function called under the stamp.
SELECT VALUE (SELECT VALUE id FROM item WHERE kind = "a" ORDER BY id) FROM anchor:0 VERSION $ts;
SELECT VALUE fn::kind_a() FROM anchor:0 VERSION $ts;
-- A subquery that selects through an indexed column.
SELECT VALUE id FROM anchor WHERE k IN (SELECT VALUE kind FROM item WHERE rank >= 4) ORDER BY id VERSION $ts;
SELECT VALUE id FROM anchor WITH NOINDEX WHERE k IN (SELECT VALUE kind FROM item WITH NOINDEX WHERE rank >= 4) ORDER BY id VERSION $ts;
-- Array elements.
SELECT VALUE id FROM item WHERE tags CONTAINS "x" ORDER BY id VERSION $ts;
SELECT VALUE id FROM item WITH NOINDEX WHERE tags CONTAINS "x" ORDER BY id VERSION $ts;
SELECT VALUE id FROM item WHERE tags CONTAINSANY ["x", "z"] ORDER BY id VERSION $ts;
SELECT VALUE id FROM item WITH NOINDEX WHERE tags CONTAINSANY ["x", "z"] ORDER BY id VERSION $ts;
-- A projection the index could answer alone, and a grouping on the indexed column.
SELECT VALUE rank FROM item WHERE rank > 1 ORDER BY rank VERSION $ts;
SELECT VALUE rank FROM item WITH NOINDEX WHERE rank > 1 ORDER BY rank VERSION $ts;
SELECT kind, count() AS n FROM item GROUP BY kind ORDER BY kind VERSION $ts;
SELECT kind, count() AS n FROM item WITH NOINDEX GROUP BY kind ORDER BY kind VERSION $ts;
-- Several sources.
SELECT VALUE id FROM item, anchor WHERE kind = "a" OR k = "a" ORDER BY id VERSION $ts;
SELECT VALUE id FROM item, anchor WITH NOINDEX WHERE kind = "a" OR k = "a" ORDER BY id VERSION $ts;
-- A pinned index is not read either.
SELECT VALUE id FROM item WITH INDEX item_kind WHERE kind = "a" ORDER BY id VERSION $ts;
-- The plan is a scan at the stamp.
EXPLAIN SELECT VALUE id FROM item WHERE kind = "a" ORDER BY id VERSION $ts;
