/**
[env]

[test]
reason = "Test limit pushdown to IndexScan when index covers ORDER BY"

[[test.results]]
value = "'OK'"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    IndexScan [ctx: Db] [table: record, index: idx_number, direction: Forward, limit: 3]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    IndexScan [ctx: Db] [table: record, index: idx_number, access: >15, direction: Forward, limit: 2]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    TableScan [ctx: Db] [table: record, direction: Forward, limit: 3]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    IndexScan [ctx: Db] [table: record, index: idx_number, direction: Forward]
''''

[[test.results]]
value = "[{ id: record:2, number: 10 }, { id: record:5, number: 20 }, { id: record:3, number: 30 }]"

[[test.results]]
value = "[{ id: record:5, number: 20 }, { id: record:3, number: 30 }, { id: record:4, number: 40 }]"

*/

-- Setup: create table with an index on 'number', declared a scalar kind so
-- the index holds one entry per record and a scan can stop at the limit
{
    DEFINE FIELD number ON TABLE record TYPE int;
    DEFINE INDEX idx_number ON TABLE record COLUMNS number;
    CREATE record:1 SET number = 50;
    CREATE record:2 SET number = 10;
    CREATE record:3 SET number = 30;
    CREATE record:4 SET number = 40;
    CREATE record:5 SET number = 20;
    RETURN "OK";
};

-- Case 1: ORDER BY indexed field with LIMIT
-- Should: IndexScan with limit pushed, sort eliminated
EXPLAIN SELECT * FROM record ORDER BY number ASC LIMIT 3;

-- Case 2: No ORDER BY, with LIMIT and index on WHERE
-- Should: IndexScan with limit pushed
EXPLAIN SELECT * FROM record WHERE number > 15 LIMIT 2;

-- Case 3: ORDER BY id with LIMIT
-- Should: TableScan in id order, no Sort, LIMIT above the scan
EXPLAIN SELECT * FROM record ORDER BY id LIMIT 3;

-- Case 4: ORDER BY indexed field, no LIMIT
-- Should: IndexScan, sort eliminated, no limit
EXPLAIN SELECT * FROM record ORDER BY number ASC;

-- Case 5: Verify data correctness - ORDER BY indexed field ASC LIMIT 3
SELECT * FROM record ORDER BY number ASC LIMIT 3;

-- Case 6: Verify data correctness - ORDER BY indexed field ASC LIMIT 3 START 1
SELECT * FROM record ORDER BY number ASC LIMIT 3 START 1;
