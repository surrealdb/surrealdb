/**
[env]
namespace = true
database = true

[test]
reason = "A GROUP ALL count produces one row, and LIMIT/START page that row like any other"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 5 }]"

*/

{
	DEFINE INDEX idx ON t FIELDS a;
	CREATE t:1 SET a = 5;
	CREATE t:2 SET a = 6;
	RETURN 'OK';
};

-- Counting a whole table.
SELECT count() FROM t GROUP ALL;
SELECT count() FROM t GROUP ALL LIMIT 1;
SELECT count() FROM t GROUP ALL LIMIT 0;
SELECT count() FROM t GROUP ALL START 1;
SELECT count() FROM t GROUP ALL LIMIT 1 START 0;

-- Counting an indexed predicate.
SELECT count() FROM t WHERE a > 0 GROUP ALL;
SELECT count() FROM t WHERE a > 0 GROUP ALL LIMIT 0;
SELECT count() FROM t WHERE a > 0 GROUP ALL START 1;

-- A single record id, which pages the one row it reads.
SELECT a FROM t:1 LIMIT 0;
SELECT a FROM t:1 START 1;
SELECT a FROM t:1 LIMIT 1;
