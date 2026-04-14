/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Defining a view table should work regardless of whether id is explicitly selected or aliased"
issue = 7134

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ i: 1, id: t:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ i: 1, id: t_view:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ i: 1, id: t_view2:1, mid: t:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
*/

-- Setup: Define a schemaful table with data
DEFINE TABLE t SCHEMAFUL TYPE NORMAL;
DEFINE FIELD i ON t TYPE int;
CREATE t:1 SET i = 1;

-- View that does not explicitly select id (id injected automatically)
DEFINE TABLE IF NOT EXISTS t_view TYPE NORMAL AS
	SELECT
		i
	FROM t;
SELECT * FROM t_view;
REMOVE TABLE t_view;

-- View that aliases id (should not crash)
DEFINE TABLE IF NOT EXISTS t_view2 TYPE NORMAL AS
	SELECT
		id as mid,
		i
	FROM t;
SELECT * FROM t_view2;
REMOVE TABLE t_view2;

REMOVE TABLE t;
