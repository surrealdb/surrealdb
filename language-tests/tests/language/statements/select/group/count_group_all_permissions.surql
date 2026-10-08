/**
[env]

imports = ["language/statements/select/group/count_group_all_permissions_import.surql"]
auth = { namespace = "test", database = "test", access = "test", rid = "table_full:baz" }

[test]
reason = "Test COUNT() GROUP ALL with different table permission levels"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[]"

*/

-- NONE permissions: no record is visible, so the count is of nothing —
-- GROUP ALL reports that as a row, as it does for every other way of
-- reaching no rows, and the ungrouped form has no row to return.
SELECT COUNT() FROM table_none GROUP ALL;
SELECT COUNT() FROM table_none:a..z;

-- FULL permissions: record is visible
SELECT COUNT() FROM table_full GROUP ALL;
SELECT COUNT() FROM table_full:a..z;

-- WHERE FALSE permissions: the same answers, reached by a per-record
-- predicate rather than by a table-level denial.
SELECT COUNT() FROM table_where_false GROUP ALL;
SELECT COUNT() FROM table_where_false:a..z;
