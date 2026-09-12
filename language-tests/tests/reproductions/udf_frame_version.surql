/**
[env]
versioned = true
timeout = 5000

[test]
reason = """
	A legacy evaluation reached from inside a versioned statement reads at the
	statement's `VERSION`. A user-defined function whose body has no logical
	form (here, a mock record set outside FROM) runs on the legacy evaluator
	when the streaming engine calls it, so the version the streaming context
	reads at has to be carried into the options that evaluation runs with;
	otherwise the body's `SELECT` reads current data while the statement
	around it reads the past.
"""

[[test.results]]
value = "'OK'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'OK'"

# 5: without VERSION the body reads the current row.
[[test.results]]
value = "[{ city: 'London' }]"

# 6: with VERSION the body reads the row as it was at $t1.
[[test.results]]
value = "[{ city: 'Paris' }]"
*/

{
	DEFINE TABLE person SCHEMALESS;
	DEFINE TABLE address SCHEMALESS;
	DEFINE FUNCTION fn::city_of($r: record) {
		LET $legacy_only = |mock:1|;
		RETURN (SELECT VALUE city FROM ONLY $r);
	};
	CREATE address:home SET city = 'Paris';
	CREATE person:alice SET address = address:home;
	RETURN "OK";
};

SLEEP 50ms;
LET $t1 = time::now();
SLEEP 50ms;

{
	UPDATE address:home SET city = 'London';
	RETURN "OK";
};

-- 5: current data.
SELECT fn::city_of(address) AS city FROM person:alice;

-- 6: the body's read is versioned along with the statement.
SELECT fn::city_of(address) AS city FROM person:alice VERSION $t1;
