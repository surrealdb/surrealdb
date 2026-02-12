/**
[env]
imports = ["language/statements/select/group/count_range_permissions_import.surql"]
auth = { namespace = "test", database = "test", access = "test", rid = "table_full:me" }

[test]
reason = "Test COUNT() with range and different table permission levels"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 1 }, { count: 1 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

*/

-- NONE permissions: query returns empty
SELECT COUNT() FROM table_none:a..z GROUP ALL;
SELECT COUNT() FROM table_none:a..z;

-- FULL permissions: both records visible
SELECT COUNT() FROM table_full:a..z GROUP ALL;
SELECT COUNT() FROM table_full:a..z;

-- WHERE FALSE permissions: GROUP ALL returns {count: 0}, non-grouped returns empty
SELECT COUNT() FROM table_where_false:a..z GROUP ALL;
SELECT COUNT() FROM table_where_false:a..z;

-- WHERE bar = 'hello' permissions: only matching record visible
SELECT COUNT() FROM table_where_match:a..z GROUP ALL;
SELECT COUNT() FROM table_where_match:a..z;
