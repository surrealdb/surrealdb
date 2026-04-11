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
value = "[{ storage: 32 }, { storage: 30 }, { storage: 34 }]"

[[test.results]]
value = "[{ storage: 8 }, { storage: 6 }, { storage: 10 }]"

[[test.results]]
error = "The table 'nonexistent' does not exist"
*/

CREATE user:a SET name = "Alice";
CREATE user:b SET name = "Bob";
CREATE user:c SET name = "Charlie";

-- Aggregate: total bytes across all rows
SELECT storage() FROM user GROUP ALL;

-- Aggregate: with alias
SELECT storage() AS total_size FROM user GROUP ALL;

-- Aggregate: specific field
SELECT storage(name) FROM user GROUP ALL;

-- Aggregate: with WHERE filter
SELECT storage() FROM user WHERE name = "Alice" GROUP ALL;

-- Per-row: byte size of each record
SELECT storage() FROM user;

-- Per-row: byte size of each field value
SELECT storage(name) FROM user;

-- Empty table
SELECT storage() FROM nonexistent GROUP ALL;
