/**
[env]

[test]
reason = "A paged GROUP ALL count keeps its single-scan fast path and applies LIMIT/START over the row it produces"

[[test.results]]
value = "'OK'"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Limit [ctx: Db] [limit: 0]
        CountScan [ctx: Db] [source: sale]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Limit [ctx: Db] [offset: 1]
        IndexCountScan [ctx: Db] [source: sale, condition: amount > 0]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: amount]
    Limit [ctx: Db] [limit: 0]
        RecordIdScan [ctx: Db] [record_id: sale:1]
''''
*/

{
	-- Index keys are counted only for a column declared to hold no array.
	DEFINE FIELD amount ON sale TYPE int;
	DEFINE INDEX idx ON sale FIELDS amount;
	CREATE sale:1 SET amount = 100;
	CREATE sale:2 SET amount = 200;
	RETURN 'OK';
};

EXPLAIN SELECT count() FROM sale GROUP ALL LIMIT 0;
EXPLAIN SELECT count() FROM sale WHERE amount > 0 GROUP ALL START 1;
EXPLAIN SELECT amount FROM sale:1 LIMIT 0;
