/**
[env]
namespace = true
database = true
timeout = 20000

[test]
reason = """
	Rows written before a field is declared `int` and put under a `UNIQUE` index may hold arrays,
	which the index holds as one entry per element. The extreme of such a field answers as the fold
	over the rows does: the index range alone is never taken as the answer, because a row that
	holds a number among its elements is not a row whose value is a number.
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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ m: 7 }]"

[[test.results]]
value = "[{ m: 5 }]"

[[test.results]]
value = "[{ m: 5 }]"

[[test.results]]
value = "[{ m: 5 }]"

[[test.results]]
value = "[{ m: 4 }]"

[[test.results]]
value = "[{ m: 4 }]"

[[test.results]]
value = "7"

[[test.results]]
value = "5"

*/

{
	-- An array whose elements are the extremes of the integers beside it.
	CREATE a:1 SET n = [1, 100] RETURN NONE;
	CREATE a:2 SET n = 5 RETURN NONE;
	CREATE a:3 SET n = 7 RETURN NONE;
	-- An array whose elements lie below the integers beside it.
	CREATE b:1 SET n = [-50, 2] RETURN NONE;
	CREATE b:2 SET n = 5 RETURN NONE;
	-- Text, an array and an integer.
	CREATE c:1 SET n = 'x' RETURN NONE;
	CREATE c:2 SET n = [100] RETURN NONE;
	CREATE c:3 SET n = 4 RETURN NONE;
	RETURN 'OK';
};

-- The declarations, which the rows above predate.
DEFINE FIELD n ON a TYPE int;
DEFINE INDEX n ON a FIELDS n UNIQUE;
DEFINE FIELD n ON b TYPE int;
DEFINE INDEX n ON b FIELDS n UNIQUE;
DEFINE FIELD n ON c TYPE int;
DEFINE INDEX n ON c FIELDS n UNIQUE;

SELECT math::max(n) AS m FROM a GROUP ALL;
SELECT math::min(n) AS m FROM a GROUP ALL;
SELECT math::max(n) AS m FROM b GROUP ALL;
SELECT math::min(n) AS m FROM b GROUP ALL;
SELECT math::max(n) AS m FROM c GROUP ALL;
SELECT math::min(n) AS m FROM c GROUP ALL;
RETURN (SELECT VALUE math::max(n) FROM a GROUP ALL)[0];
RETURN (SELECT VALUE math::min(n) FROM b GROUP ALL)[0];
