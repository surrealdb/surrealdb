/**
[env]
imports = ["language/statements/select/group/count_group_all_permissions_import.surql"]
auth = { namespace = "test", database = "test", access = "test", rid = "table_full:baz" }

[test]
reason = "Test COUNT() GROUP ALL with different table permission levels"

[[test.results]]
value = "[]"

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

-- NONE permissions: query returns empty
SELECT COUNT() FROM table_none GROUP ALL;
SELECT COUNT() FROM table_none:a..z;

-- FULL permissions: record is visible
SELECT COUNT() FROM table_full GROUP ALL;
SELECT COUNT() FROM table_full:a..z;

-- WHERE FALSE permissions: GROUP ALL returns {count: 0}, non-grouped returns empty
SELECT COUNT() FROM table_where_false GROUP ALL;
SELECT COUNT() FROM table_where_false:a..z;
