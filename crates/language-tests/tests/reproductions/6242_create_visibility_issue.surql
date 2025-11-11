/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: relationship_node_plain:0123456789abcdef0123 }]"
skip-record-id-key = true

[[test.results]]
value = "[{ id: relationship_node_plain:0123456789abcdef0123, predicate: 'p1' }]"
skip-record-id-key = true

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ id: relationship_node_plain:`'explicit_id'` }]"

[[test.results]]
value = "[{ id: relationship_node_plain:`'explicit_id'`, predicate: 'p2' }]"

[[test.results]]
value = "[{ count: 1 }, { count: 1 }]"

*/

-- Reproduction test for issue #6242: CREATE operations visibility
-- https://github.com/surrealdb/surrealdb/issues/6242
-- This test verifies that CREATE operations work correctly and return visible records

-- Define the table
DEFINE TABLE relationship_node_plain;

-- Test 1: Basic CREATE with RETURN id should return the created record ID
CREATE relationship_node_plain SET predicate='p1' RETURN id;

-- Test 2: SELECT should show the created record
SELECT id, predicate FROM relationship_node_plain;

-- Test 3: Count should match the visible records
SELECT count() FROM relationship_node_plain;

-- Test 4: CREATE with explicit ID should work
CREATE relationship_node_plain:`'explicit_id'` SET predicate='p2' RETURN id;

-- Test 5: SELECT should show both records including the explicit ID
SELECT id, predicate FROM relationship_node_plain:`'explicit_id'`;

-- Test 6: Final count should show both records
SELECT count() FROM relationship_node_plain;
