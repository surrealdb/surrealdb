/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "SELECT count() GROUP ALL should use the COUNT index fast path for both unconditional and conditional COUNT indexes"

[[test.results]]
value = "None"

[[test.results]]
value = "None"

[[test.results]]
value = "[{ id: item:1, status: 'active' }]"

[[test.results]]
value = "[{ id: item:2, status: 'active' }]"

[[test.results]]
value = "[{ id: item:3, status: 'active' }]"

[[test.results]]
value = "[{ id: item:4, status: 'inactive' }]"

[[test.results]]
value = "[{ id: item:5, status: 'inactive' }]"

[[test.results]]
value = "[{ count: 5 }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "[{ count: 2 }]"
*/

-- Unconditional COUNT index: tracks total row count
DEFINE INDEX idx_total ON item COUNT;

-- Conditional COUNT index: tracks only active rows
DEFINE INDEX idx_active_count ON item COUNT WHERE status = "active";

-- Insert test data
CREATE item:1 SET status = "active";
CREATE item:2 SET status = "active";
CREATE item:3 SET status = "active";
CREATE item:4 SET status = "inactive";
CREATE item:5 SET status = "inactive";

-- Test 1: unconditional count (uses idx_total)
SELECT count() FROM item GROUP ALL;

-- Test 2: conditional count with matching WHERE (uses idx_active_count)
SELECT count() FROM item WHERE status = "active" GROUP ALL;

-- Test 3: conditional count with non-matching WHERE (no index, full scan)
SELECT count() FROM item WHERE status = "inactive" GROUP ALL;

-- Delete one active record
DELETE item:1;

-- Test 4: unconditional count after delete
SELECT count() FROM item GROUP ALL;

-- Test 5: conditional count after delete
SELECT count() FROM item WHERE status = "active" GROUP ALL;
