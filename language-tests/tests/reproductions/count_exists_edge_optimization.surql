/**
[env]
namespace = true
database = true
auth = { level = "owner" }
planner-strategy = ["all-ro"]

[test]
reason = "count(->edge) OP N in WHERE should inject a minimal LIMIT into graph traversal"

[[test.results]]
value = "[{ id: node:a, name: 'A' }]"

[[test.results]]
value = "[{ id: node:b, name: 'B' }]"

[[test.results]]
value = "[{ id: node:c, name: 'C' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: name] {rows: 1}
    TableScan [ctx: Db] [table: node, direction: Forward, predicate: count(...) > 0] {rows: 1}

Total rows: 1''''

[[test.results]]
value = "[{ name: 'A' }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: name] {rows: 1}
    TableScan [ctx: Db] [table: node, direction: Forward, predicate: count(...) >= 1] {rows: 1}

Total rows: 1''''

[[test.results]]
value = "[{ name: 'A' }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: name] {rows: 2}
    TableScan [ctx: Db] [table: node, direction: Forward, predicate: count(...) = 0] {rows: 2}

Total rows: 2''''

[[test.results]]
value = "[{ name: 'B' }, { name: 'C' }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: name] {rows: 1}
    TableScan [ctx: Db] [table: node, direction: Forward, predicate: count(...) > 1] {rows: 1}

Total rows: 1''''

[[test.results]]
value = "[{ name: 'A' }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: name] {rows: 1}
    TableScan [ctx: Db] [table: node, direction: Forward, predicate: count(...) >= 2] {rows: 1}

Total rows: 1''''

[[test.results]]
value = "[{ name: 'A' }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: name] {rows: 1}
    TableScan [ctx: Db] [table: node, direction: Forward, predicate: count(...) = 2] {rows: 1}

Total rows: 1''''

[[test.results]]
value = "[{ name: 'A' }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: name] {rows: 1}
    TableScan [ctx: Db] [table: node, direction: Forward, predicate: count(...) != 0] {rows: 1}

Total rows: 1''''

[[test.results]]
value = "[{ name: 'A' }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: name] {rows: 2}
    TableScan [ctx: Db] [table: node, direction: Forward, predicate: count(...) < 2] {rows: 2}

Total rows: 2''''

[[test.results]]
value = "[{ name: 'B' }, { name: 'C' }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: name] {rows: 2}
    TableScan [ctx: Db] [table: node, direction: Forward, predicate: count(...) <= 1] {rows: 2}

Total rows: 2''''

[[test.results]]
value = "[{ name: 'B' }, { name: 'C' }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: name] {rows: 1}
    TableScan [ctx: Db] [table: node, direction: Forward, predicate: 1 < count(...)] {rows: 1}

Total rows: 1''''

[[test.results]]
value = "[{ name: 'A' }]"

*/

-- Setup: create nodes and edges
-- node:a has 2 outgoing edges, node:b and node:c have 0
CREATE node:a SET name = "A";
CREATE node:b SET name = "B";
CREATE node:c SET name = "C";
LET $_ = (RELATE node:a->connects->node:b);
LET $_ = (RELATE node:a->connects->node:c);

-- Existing: count > 0 (LIMIT 1)
EXPLAIN ANALYZE SELECT name FROM node WHERE count(->connects) > 0;
SELECT name FROM node WHERE count(->connects) > 0;

-- Existing: count >= 1 (LIMIT 1)
EXPLAIN ANALYZE SELECT name FROM node WHERE count(->connects) >= 1;
SELECT name FROM node WHERE count(->connects) >= 1;

-- count = 0 (LIMIT 1 — enough to distinguish 0 from non-zero)
EXPLAIN ANALYZE SELECT name FROM node WHERE count(->connects) = 0;
SELECT name FROM node WHERE count(->connects) = 0;

-- count > 1 (LIMIT 2)
EXPLAIN ANALYZE SELECT name FROM node WHERE count(->connects) > 1;
SELECT name FROM node WHERE count(->connects) > 1;

-- count >= 2 (LIMIT 2)
EXPLAIN ANALYZE SELECT name FROM node WHERE count(->connects) >= 2;
SELECT name FROM node WHERE count(->connects) >= 2;

-- count = 2 (LIMIT 3)
EXPLAIN ANALYZE SELECT name FROM node WHERE count(->connects) = 2;
SELECT name FROM node WHERE count(->connects) = 2;

-- count != 0 (LIMIT 1)
EXPLAIN ANALYZE SELECT name FROM node WHERE count(->connects) != 0;
SELECT name FROM node WHERE count(->connects) != 0;

-- count < 2 (LIMIT 2)
EXPLAIN ANALYZE SELECT name FROM node WHERE count(->connects) < 2;
SELECT name FROM node WHERE count(->connects) < 2;

-- count <= 1 (LIMIT 2)
EXPLAIN ANALYZE SELECT name FROM node WHERE count(->connects) <= 1;
SELECT name FROM node WHERE count(->connects) <= 1;

-- Reversed form: 1 < count (LIMIT 2)
EXPLAIN ANALYZE SELECT name FROM node WHERE 1 < count(->connects);
SELECT name FROM node WHERE 1 < count(->connects);
