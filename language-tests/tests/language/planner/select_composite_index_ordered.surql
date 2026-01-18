/**
[test]
reason = "Test that composite indexes with equality on leading columns provide ordering for trailing columns, avoiding unnecessary memory sorting"
issue = 6700

[[test.results]]
value = "'OK'"

# Test 1: Composite index with equality on first column, ORDER BY second column + LIMIT
# Should use Memory collector (not MemoryOrderedLimit) since index provides order
[[test.results]]
value = "[{ detail: { plan: { index: 'idx_status_name', operator: '=', value: 'active' }, table: 'account' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }, { detail: { type: 'KeysAndValues' }, operation: 'RecordStrategy' }, { detail: { CancelOnLimit: 3 }, operation: 'StartLimitStrategy' }, { detail: { count: 3 }, operation: 'Fetch' }]"

[[test.results]]
value = "[{ id: account:1, name: 'Alice', status: 'active' }, { id: account:2, name: 'Bob', status: 'active' }, { id: account:3, name: 'Charlie', status: 'active' }]"

# Test 2: Same query without LIMIT - should still use Memory collector (not MemoryOrdered)
[[test.results]]
value = "[{ detail: { plan: { index: 'idx_status_name', operator: '=', value: 'active' }, table: 'account' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }, { detail: { type: 'KeysAndValues' }, operation: 'RecordStrategy' }, { detail: { count: 4 }, operation: 'Fetch' }]"

[[test.results]]
value = "[{ id: account:1, name: 'Alice', status: 'active' }, { id: account:2, name: 'Bob', status: 'active' }, { id: account:3, name: 'Charlie', status: 'active' }, { id: account:4, name: 'Diana', status: 'active' }]"

# Test 3: ORDER BY DESC with LIMIT - index provides reverse order
[[test.results]]
value = "[{ detail: { plan: { index: 'idx_status_name', operator: '=', value: 'active' }, table: 'account' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }, { detail: { type: 'KeysAndValues' }, operation: 'RecordStrategy' }, { detail: { CancelOnLimit: 2 }, operation: 'StartLimitStrategy' }, { detail: { count: 2 }, operation: 'Fetch' }]"

[[test.results]]
value = "[{ id: account:4, name: 'Diana', status: 'active' }, { id: account:3, name: 'Charlie', status: 'active' }]"

# Test 4: Verify inactive status also uses Memory collector
[[test.results]]
value = "[{ detail: { plan: { index: 'idx_status_name', operator: '=', value: 'inactive' }, table: 'account' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }, { detail: { type: 'KeysAndValues' }, operation: 'RecordStrategy' }, { detail: { CancelOnLimit: 2 }, operation: 'StartLimitStrategy' }, { detail: { count: 2 }, operation: 'Fetch' }]"

[[test.results]]
value = "[{ id: account:5, name: 'Eve', status: 'inactive' }, { id: account:6, name: 'Frank', status: 'inactive' }]"

*/

-- Setup: Create table with composite index on (status, name)
-- This index can satisfy both WHERE status = X and ORDER BY name
{
    DEFINE INDEX idx_status_name ON TABLE account COLUMNS status, name;
    
    -- Create test data with various statuses
    CREATE account:1 SET status = 'active', name = 'Alice';
    CREATE account:2 SET status = 'active', name = 'Bob';
    CREATE account:3 SET status = 'active', name = 'Charlie';
    CREATE account:4 SET status = 'active', name = 'Diana';
    CREATE account:5 SET status = 'inactive', name = 'Eve';
    CREATE account:6 SET status = 'inactive', name = 'Frank';
    
    RETURN "OK";
};

-- Test 1: Equality on first column + ORDER BY second column + LIMIT
-- The composite index (status, name) provides records in name order for a given status
-- So no memory sorting should be needed - collector should be Memory, not MemoryOrderedLimit
SELECT * FROM account WHERE status = 'active' ORDER BY name ASC LIMIT 3 EXPLAIN FULL;
SELECT * FROM account WHERE status = 'active' ORDER BY name ASC LIMIT 3;

-- Test 2: Same without LIMIT - should use Memory collector (not MemoryOrdered)
SELECT * FROM account WHERE status = 'active' ORDER BY name ASC EXPLAIN FULL;
SELECT * FROM account WHERE status = 'active' ORDER BY name ASC;

-- Test 3: ORDER BY DESC with LIMIT
SELECT * FROM account WHERE status = 'active' ORDER BY name DESC LIMIT 2 EXPLAIN FULL;
SELECT * FROM account WHERE status = 'active' ORDER BY name DESC LIMIT 2;

-- Test 4: Different equality value
SELECT * FROM account WHERE status = 'inactive' ORDER BY name ASC LIMIT 2 EXPLAIN FULL;
SELECT * FROM account WHERE status = 'inactive' ORDER BY name ASC LIMIT 2;
