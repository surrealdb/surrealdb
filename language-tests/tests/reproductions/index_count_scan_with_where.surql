/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "SELECT count() ... WHERE ... GROUP ALL should use IndexCountScan when a matching COUNT index exists"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: item:a, name: 'A', status: 'active' }]"

[[test.results]]
value = "[{ id: item:b, name: 'B', status: 'active' }]"

[[test.results]]
value = "[{ id: item:c, name: 'C', status: 'active' }]"

[[test.results]]
value = "[{ id: item:d, name: 'D', status: 'inactive' }]"

[[test.results]]
value = "[{ id: item:e, name: 'E', status: 'inactive' }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ c: 3 }]"
*/

-- Define the COUNT index before inserting data so deltas are tracked from the start
DEFINE INDEX item_active_count ON item COUNT WHERE status = "active";

-- Setup: create test records with a status field
CREATE item:a SET status = "active", name = "A";
CREATE item:b SET status = "active", name = "B";
CREATE item:c SET status = "active", name = "C";
CREATE item:d SET status = "inactive", name = "D";
CREATE item:e SET status = "inactive", name = "E";

-- Test: count with matching WHERE should use IndexCountScan fast path
SELECT count() FROM item WHERE status = "active" GROUP ALL;

-- Test: count with a different WHERE condition falls back to scan
SELECT count() FROM item WHERE status = "inactive" GROUP ALL;

-- Test: count with alias and matching WHERE
SELECT count() AS c FROM item WHERE status = "active" GROUP ALL;
