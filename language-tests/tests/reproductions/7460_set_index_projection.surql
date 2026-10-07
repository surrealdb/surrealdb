/**
[env]
namespace = true
database = true
auth = { level = "owner" }
planner-strategy = ["all-ro"]

[test]
reason = "Wildcard indexes on sets must return the same membership matches as array indexes and table scans."
issue = 7460

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
value = "[{ arr_val: [5058, 1234], id: test_data:1, set_val: {1234, 5058} }]"

[[test.results]]
value = "[{ arr_val: [5058, 9999], id: test_data:2, set_val: {5058, 9999} }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: test_data:1 }, { id: test_data:2 }]"

[[test.results]]
value = "[{ id: test_data:1 }, { id: test_data:2 }]"

[[test.results]]
value = "[{ id: test_data:1 }, { id: test_data:2 }]"

[[test.results]]
value = "[{ id: test_data:1 }, { id: test_data:2 }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: id]\n    IndexScan [ctx: Db] [index: idx_arr, access: = 5058, direction: Forward]\n''''

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: id]\n    IndexScan [ctx: Db] [index: idx_set, access: = 5058, direction: Forward]\n''''

[[test.results]]
value = "[{ id: test_data:4 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test_data:1 }]"

[[test.results]]
value = "[{ id: test_data:1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: test_data:2 }]"

[[test.results]]
value = "[{ id: test_data:2 }]"

[[test.results]]
value = "[{ id: test_data:1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test_data:1 }]"
[[test.results]]
value = "NONE"
[[test.results]]
value = "[{ id: test_data:1 }]"
[[test.results]]
match = "$result CONTAINS 'IndexScan' AND $result CONTAINS 'idx_compound'"
error = false
[[test.results]]
value = "NONE"
[[test.results]]
value = "NONE"
[[test.results]]
value = "[]"
[[test.results]]
value = "[{ id: nested:a }]"
[[test.results]]
match = "$result CONTAINS 'IndexScan' AND $result CONTAINS 'idx_nested'"
error = false
*/

DEFINE TABLE test_data SCHEMALESS;
DEFINE FIELD arr_val ON test_data TYPE array<int>;
DEFINE FIELD set_val ON test_data TYPE set<int>;
DEFINE INDEX idx_arr ON test_data COLUMNS arr_val.*;
DEFINE INDEX idx_set ON test_data COLUMNS set_val.*;

CREATE test_data:1 CONTENT {
    arr_val: [5058, 1234],
    set_val: <set>[5058, 1234]
};
CREATE test_data:2 CONTENT {
    arr_val: [5058, 9999],
    set_val: <set>[5058, 9999]
};
CREATE test_data:3 CONTENT { arr_val: [], set_val: <set>[] } RETURN NONE;
CREATE test_data:4 CONTENT { arr_val: [7777], set_val: <set>[7777, 7777] } RETURN NONE;

SELECT id FROM test_data WHERE arr_val CONTAINS 5058 ORDER BY id;
SELECT id FROM test_data WHERE set_val CONTAINS 5058 ORDER BY id;
SELECT id FROM test_data WITH NOINDEX WHERE arr_val CONTAINS 5058 ORDER BY id;
SELECT id FROM test_data WITH NOINDEX WHERE set_val CONTAINS 5058 ORDER BY id;

EXPLAIN SELECT id FROM test_data WHERE arr_val CONTAINS 5058;
EXPLAIN SELECT id FROM test_data WHERE set_val CONTAINS 5058;

-- Empty and nonmatching sets must not produce spurious matches.
SELECT id FROM test_data WHERE set_val CONTAINS 7777;
SELECT id FROM test_data WHERE set_val CONTAINS 42;

-- An index without a wildcard still stores the entire set as one key.
DEFINE INDEX idx_whole_set ON test_data COLUMNS set_val;
SELECT id FROM test_data WHERE set_val = {1234, 5058};
SELECT id FROM test_data WITH NOINDEX WHERE set_val = {1234, 5058};

-- Mutations must remove the old element keys and insert the new ones.
UPDATE test_data:1 SET set_val = <set>[111, 1234] RETURN NONE;
SELECT id FROM test_data WHERE set_val CONTAINS 5058;
SELECT id FROM test_data WITH NOINDEX WHERE set_val CONTAINS 5058;
SELECT id FROM test_data WHERE set_val CONTAINS 111;
DELETE test_data:2 RETURN NONE;
SELECT id FROM test_data WHERE set_val CONTAINS 5058;

-- Rebuilding on populated data must use the same element expansion.
REBUILD INDEX idx_set ON test_data;
SELECT id FROM test_data WHERE set_val CONTAINS 111;

-- A bound leading wildcard may still use a compound index.
DEFINE INDEX idx_compound ON test_data COLUMNS set_val.*, arr_val…;
SELECT id FROM test_data WITH INDEX idx_compound WHERE set_val CONTAINS 111;
EXPLAIN SELECT id FROM test_data WITH INDEX idx_compound WHERE set_val CONTAINS 111;

-- Plain nested object fields still support element index seeks.
DEFINE FIELD obj.tags ON nested TYPE set<int>;
DEFINE INDEX idx_nested ON nested FIELDS obj.tags.*;
CREATE nested:a SET obj = { tags: {1, 2} } RETURN NONE;
SELECT id FROM nested WHERE obj.tags CONTAINS 1;
EXPLAIN SELECT id FROM nested WHERE obj.tags CONTAINS 1;
