/**
[test]
reason = "Tests ORDER BY and LIMIT clause planning in the new executor"

[env]
ns = "test"
db = "test"

*/

-- Setup: Create test table with data
DEFINE TABLE person SCHEMALESS;

-- Insert test data
CREATE person:1 SET name = "Alice", age = 30;
CREATE person:2 SET name = "Bob", age = 25;
CREATE person:3 SET name = "Charlie", age = 35;
CREATE person:4 SET name = "Diana", age = 28;
CREATE person:5 SET name = "Eve", age = 32;

-- Test: Simple ORDER BY ascending
SELECT * FROM person ORDER BY age ASC;

-- Test: Simple ORDER BY descending
SELECT * FROM person ORDER BY age DESC;

-- Test: ORDER BY with LIMIT
SELECT * FROM person ORDER BY age ASC LIMIT 3;

-- Test: ORDER BY with START (offset)
SELECT * FROM person ORDER BY age ASC START 2;

-- Test: ORDER BY with LIMIT and START
SELECT * FROM person ORDER BY age ASC LIMIT 2 START 1;

-- Test: Multiple ORDER BY fields
SELECT * FROM person ORDER BY age ASC, name DESC;

-- Test: LIMIT without ORDER BY
SELECT * FROM person LIMIT 2;

-- Test: START without LIMIT
SELECT * FROM person START 3;
