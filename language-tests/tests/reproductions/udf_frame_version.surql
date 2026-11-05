/**
[env]
versioned = true
timeout = 5000

[test]
reason = """
	A user-defined function body evaluated from inside a versioned statement reads
	at the statement's `VERSION`: the body's `SELECT` reads the past data the
	statement around it reads, not current data. Here the body has no logical
	form (a mock record set outside FROM), so the version of the calling context
	has to be carried into the evaluation of the body.
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
