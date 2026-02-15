/**
[test]
reason = "Test EXPLAIN with index-backed queries: standard index, unique index, compound index"

[[test.results]]
value = "'OK'"

[[test.results]]
value = '''"Project [ctx: Db] {rows: 1, batches: 1}
└────> Filter [ctx: Db] [predicate: email = 'a@test.com'] {rows: 1, batches: 1}
       └────> IndexScan [ctx: Db] [index: idx_email_uniq, access: = 'a@test.com', direction: Forward] {rows: 1, batches: 1}

Total rows: 1"'''

[[test.results]]
value = '''"Project [ctx: Db] {rows: 2, batches: 1}
└────> Filter [ctx: Db] [predicate: region = 'EU' AND status = 'active'] {rows: 2, batches: 1}
       └────> IndexScan [ctx: Db] [index: idx_region_status, access: ['EU', 'active'], direction: Forward] {rows: 2, batches: 1}

Total rows: 2"'''

[[test.results]]
value = '''"Project [ctx: Db] {rows: 1, batches: 1}
└────> TableScan [ctx: Db] [table: user, direction: Forward, predicate: email = 'a@test.com'] {rows: 1, batches: 1}

Total rows: 1"'''

[[test.results]]
value = '''"Project [ctx: Db] {rows: 2, batches: 1}
└────> Filter [ctx: Db] [predicate: region = 'NA'] {rows: 2, batches: 1}
       └────> IndexScan [ctx: Db] [index: idx_region_status, access: ['NA'], direction: Forward] {rows: 2, batches: 1}

Total rows: 2"'''
*/

-- Setup schema and data
{
	DEFINE INDEX idx_email ON user FIELDS email;
	DEFINE INDEX idx_email_uniq ON user FIELDS email UNIQUE;
	DEFINE INDEX idx_region_status ON user FIELDS region, status;
	CREATE user:1 SET email = 'a@test.com', region = 'NA', status = 'active';
	CREATE user:2 SET email = 'b@test.com', region = 'NA', status = 'inactive';
	CREATE user:3 SET email = 'c@test.com', region = 'EU', status = 'active';
	CREATE user:4 SET email = 'd@test.com', region = 'EU', status = 'active';
	RETURN "OK";
};

-- EXPLAIN a query that can use a standard index
EXPLAIN ANALYZE SELECT * FROM user WHERE email = 'a@test.com';

-- EXPLAIN with a compound index
EXPLAIN ANALYZE SELECT * FROM user WHERE region = 'EU' AND status = 'active';

-- EXPLAIN with WITH NOINDEX to force table scan
EXPLAIN ANALYZE SELECT * FROM user WITH NOINDEX WHERE email = 'a@test.com';

-- EXPLAIN a range scan on the compound index
EXPLAIN ANALYZE SELECT * FROM user WHERE region = 'NA';
