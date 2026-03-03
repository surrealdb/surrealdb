/**
[test]
reason = "Test EXPLAIN with ORDER BY, LIMIT, and START clauses"

[[test.results]]
value = "'OK'"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]
    SortByKey [ctx: Db] [sort_keys: val ASC]
        TableScan [ctx: Db] [table: record, direction: Forward]
''''

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]
    SortByKey [ctx: Db] [sort_keys: val DESC]
        TableScan [ctx: Db] [table: record, direction: Forward]
''''

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]
    TableScan [ctx: Db] [table: record, direction: Forward, limit: 5]
''''

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]
    TableScan [ctx: Db] [table: record, direction: Forward, limit: 3, offset: 2]
''''

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]
    Limit [ctx: Db] [limit: 2, offset: 1]
        SortTopKByKey [ctx: Db] [sort_keys: val DESC, limit: 3]
            TableScan [ctx: Db] [table: record, direction: Forward]
''''
*/

-- Setup test data
{
	FOR $i IN 1..11 {
		CREATE record CONTENT { val: $i };
	};
	RETURN "OK";
};

-- EXPLAIN ORDER BY ascending
EXPLAIN SELECT * FROM record ORDER BY val ASC;

-- EXPLAIN ORDER BY descending
EXPLAIN SELECT * FROM record ORDER BY val DESC;

-- EXPLAIN with LIMIT only
EXPLAIN SELECT * FROM record LIMIT 5;

-- EXPLAIN with LIMIT and START
EXPLAIN SELECT * FROM record LIMIT 3 START 2;

-- EXPLAIN combining ORDER BY, LIMIT, and START
EXPLAIN SELECT * FROM record ORDER BY val DESC LIMIT 2 START 1;
