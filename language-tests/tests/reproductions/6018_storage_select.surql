/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "STORAGE() aggregate function to calculate serialized byte size of records"
issue = 6018

[[test.results]]
value = "[{ id: user:a, name: 'Alice' }]"

[[test.results]]
value = "[{ id: user:b, name: 'Bob' }]"

[[test.results]]
value = "[{ id: user:c, name: 'Charlie' }]"

[[test.results]]
value = "[{ storage: 96 }]"

[[test.results]]
value = "[{ total_size: 96 }]"

[[test.results]]
value = "[{ storage: 24 }]"

[[test.results]]
value = "[{ storage: 32 }]"

[[test.results]]
error = "The table 'nonexistent' does not exist"
*/

-- Setup: create test records
CREATE user:a SET name = "Alice";
CREATE user:b SET name = "Bob";
CREATE user:c SET name = "Charlie";

-- Test 1: SELECT storage() FROM table GROUP ALL (total bytes)
SELECT storage() FROM user GROUP ALL;

-- Test 2: SELECT storage() with alias
SELECT storage() AS total_size FROM user GROUP ALL;

-- Test 3: SELECT storage(name) to get size of specific field
SELECT storage(name) FROM user GROUP ALL;

-- Test 4: SELECT storage() with WHERE filter
SELECT storage() FROM user WHERE name = "Alice" GROUP ALL;

-- Test 5: Empty table
SELECT storage() FROM nonexistent GROUP ALL;
