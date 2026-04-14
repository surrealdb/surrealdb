/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Defining a view table that aliases the id field should not cause an unreachable logic error"
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
*/

-- Setup: Define a schemaful table with data
DEFINE TABLE t SCHEMAFUL TYPE NORMAL;
DEFINE FIELD i ON t TYPE int;
CREATE t:1 SET i = 1;

-- Bug trigger: Define a view that aliases id to mid (no raw 'id' in output)
DEFINE TABLE IF NOT EXISTS t_view TYPE NORMAL AS
	SELECT
		id as mid,
		i
	FROM t;

-- Verify the view was created with data
SELECT * FROM t_view;

-- Cleanup (must remove view first since it depends on t)
REMOVE TABLE t_view;
REMOVE TABLE t;
