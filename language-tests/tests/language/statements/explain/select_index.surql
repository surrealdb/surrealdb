/**
[test]
reason = "Test EXPLAIN with index-backed queries: standard index, unique index, compound index"

[[test.results]]
value = "'OK'"

[[test.results]]
value = '''"Project [ctx: Db]\n└────> Scan [ctx: Db] [source: user, predicate: email = 'a@test.com']\n"'''

[[test.results]]
value = '''"Project [ctx: Db]\n└────> Scan [ctx: Db] [source: user, predicate: region = 'EU' AND status = 'active']\n"'''

[[test.results]]
value = '''"Project [ctx: Db]\n└────> Scan [ctx: Db] [source: user, predicate: email = 'a@test.com']\n"'''

[[test.results]]
value = '''"Project [ctx: Db]\n└────> Scan [ctx: Db] [source: user, predicate: region = 'NA']\n"'''
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
EXPLAIN SELECT * FROM user WHERE email = 'a@test.com';

-- EXPLAIN with a compound index
EXPLAIN SELECT * FROM user WHERE region = 'EU' AND status = 'active';

-- EXPLAIN with WITH NOINDEX to force table scan
EXPLAIN SELECT * FROM user WITH NOINDEX WHERE email = 'a@test.com';

-- EXPLAIN a range scan on the compound index
EXPLAIN SELECT * FROM user WHERE region = 'NA';
