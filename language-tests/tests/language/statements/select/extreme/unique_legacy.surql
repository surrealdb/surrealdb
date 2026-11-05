/**
[env]
namespace = true
database = true
timeout = 20000

[test]
reason = """
	Rows written before a field is declared `int` and put under a `UNIQUE` index may hold what the
	declaration would refuse: text, NULL, a boolean, no value, a float above every integer, and NaN.
	The extreme of such a field answers as the fold over the rows does: values that are not numbers are skipped, and a table with no number answers an infinity. A unique index refuses to be defined over equal numbers of different kinds, so no
	such table is here.
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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ m: 1000.5f }]"

[[test.results]]
value = "[{ m: 3 }]"

[[test.results]]
value = "[{ m: -Infinity }]"
[[test.results]]
value = "[{ m: Infinity }]"
[[test.results]]
value = "[{ m: -Infinity }]"

[[test.results]]
value = "[{ m: Infinity }]"

[[test.results]]
value = "[{ m: NaN }]"

[[test.results]]
value = "[{ m: 3 }]"

[[test.results]]
value = "[{ m: -Infinity }]"
[[test.results]]
value = "[{ m: Infinity }]"
[[test.results]]
value = "[{ m: -Infinity }]"
[[test.results]]
value = "[{ m: Infinity }]"
[[test.results]]
value = "[{ m: -Infinity }]"
[[test.results]]
value = "[{ m: Infinity }]"
[[test.results]]
value = "[{ m: 3 }]"

[[test.results]]
value = "[{ m: 3 }]"

[[test.results]]
value = "1000.5f"

[[test.results]]
value = "[{ id: d:1, m: Infinity }, { id: d:2, m: Infinity }]"
[[test.results]]
value = "[{ m: 1000.5f }]"

*/

{
	-- Text, a missing value and a float above every integer beside integers.
	CREATE a:1 SET n = 'x' RETURN NONE;
	CREATE a:2 SET m = 1 RETURN NONE;
	CREATE a:3 SET n = 1000.5f RETURN NONE;
	CREATE a:4 SET n = 3 RETURN NONE;
	CREATE a:5 SET n = 8 RETURN NONE;
	-- Only text.
	CREATE c:1 SET n = 'x' RETURN NONE;
	CREATE c:2 SET n = 'y' RETURN NONE;
	-- Only no value.
	CREATE d:1 SET m = 1 RETURN NONE;
	CREATE d:2 SET m = 2 RETURN NONE;
	-- NaN beside integers.
	CREATE e:1 SET n = 3 RETURN NONE;
	CREATE e:2 SET n = NaN RETURN NONE;
	CREATE e:3 SET n = 8 RETURN NONE;
	-- Text and a boolean.
	CREATE f:1 SET n = 'y' RETURN NONE;
	CREATE f:2 SET n = true RETURN NONE;
	-- NULL and text.
	CREATE g:1 SET n = NULL RETURN NONE;
	CREATE g:2 SET n = 'x' RETURN NONE;
	-- Text.
	CREATE h:1 SET n = 'y' RETURN NONE;
	CREATE h:2 SET n = 'x' RETURN NONE;
	-- NULL and a boolean beside an integer.
	CREATE i:1 SET n = NULL RETURN NONE;
	CREATE i:2 SET n = true RETURN NONE;
	CREATE i:3 SET n = 3 RETURN NONE;
	RETURN 'OK';
};

-- The declarations, which the rows above predate.
DEFINE FIELD n ON a TYPE int;
DEFINE INDEX n ON a FIELDS n UNIQUE;
DEFINE FIELD n ON c TYPE int;
DEFINE INDEX n ON c FIELDS n UNIQUE;
DEFINE FIELD n ON d TYPE int;
DEFINE INDEX n ON d FIELDS n UNIQUE;
DEFINE FIELD n ON e TYPE int;
DEFINE INDEX n ON e FIELDS n UNIQUE;
DEFINE FIELD n ON f TYPE int;
DEFINE INDEX n ON f FIELDS n UNIQUE;
DEFINE FIELD n ON g TYPE int;
DEFINE INDEX n ON g FIELDS n UNIQUE;
DEFINE FIELD n ON h TYPE int;
DEFINE INDEX n ON h FIELDS n UNIQUE;
DEFINE FIELD n ON i TYPE int;
DEFINE INDEX n ON i FIELDS n UNIQUE;

-- Text, a missing value and a float above every integer, beside integers.
SELECT math::max(n) AS m FROM a GROUP ALL;
SELECT math::min(n) AS m FROM a GROUP ALL;
-- Only text: the first text by record id is named.
SELECT math::max(n) AS m FROM c GROUP ALL;
SELECT math::min(n) AS m FROM c GROUP ALL;
-- Only missing values.
SELECT math::max(n) AS m FROM d GROUP ALL;
SELECT math::min(n) AS m FROM d GROUP ALL;
-- NaN beside integers.
SELECT math::max(n) AS m FROM e GROUP ALL;
SELECT math::min(n) AS m FROM e GROUP ALL;
-- Text and a boolean.
SELECT math::max(n) AS m FROM f GROUP ALL;
SELECT math::min(n) AS m FROM f GROUP ALL;
-- NULL and text.
SELECT math::max(n) AS m FROM g GROUP ALL;
SELECT math::min(n) AS m FROM g GROUP ALL;
-- Two texts.
SELECT math::max(n) AS m FROM h GROUP ALL;
SELECT math::min(n) AS m FROM h GROUP ALL;
-- NULL and a boolean beside an integer.
SELECT math::max(n) AS m FROM i GROUP ALL;
SELECT math::min(n) AS m FROM i GROUP ALL;
-- Value position and paging of the same rows.
RETURN (SELECT VALUE math::max(n) FROM a GROUP ALL)[0];
SELECT id, (SELECT VALUE math::min(n) FROM c GROUP ALL)[0] AS m FROM d ORDER BY id;
SELECT math::max(n) AS m FROM a GROUP ALL LIMIT 1;
