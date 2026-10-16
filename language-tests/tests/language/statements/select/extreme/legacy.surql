/**
[env]
namespace = true
database = true
timeout = 20000

[test]
reason = """
	Rows written before a field is declared `int` and indexed may hold what the declaration would
	refuse: text, NULL, a boolean, no value, a float above every integer, NaN, and a float equal to
	an integer. The extreme of such a field answers as the fold over the rows does: numbers fold
	and keep the first of equal ones in scan order, NONE and NULL are skipped, and a table with no
	number answers an infinity or names the first value that is not one.
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
value = "[{ m: 5 }]"

[[test.results]]
value = "[{ m: 5f }]"

[[test.results]]
value = "[{ m: 5 }]"

[[test.results]]
value = "[{ m: 5f }]"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ m: 1 }]"

[[test.results]]
value = "[{ m: 9 }]"

[[test.results]]
error = "Incorrect arguments for function math::max(). Argument 1 was the wrong type. Expected `number` but found `'x'`"

[[test.results]]
error = "Incorrect arguments for function math::min(). Argument 1 was the wrong type. Expected `number` but found `'x'`"

[[test.results]]
value = "[{ m: -Infinity }]"

[[test.results]]
value = "[{ m: Infinity }]"

[[test.results]]
value = "[{ m: NaN }]"

[[test.results]]
value = "[{ m: 3 }]"

[[test.results]]
error = "Incorrect arguments for function math::max(). Argument 1 was the wrong type. Expected `number` but found `'y'`"

[[test.results]]
error = "Incorrect arguments for function math::min(). Argument 1 was the wrong type. Expected `number` but found `'y'`"

[[test.results]]
error = "Incorrect arguments for function math::max(). Argument 1 was the wrong type. Expected `number` but found `'x'`"

[[test.results]]
error = "Incorrect arguments for function math::min(). Argument 1 was the wrong type. Expected `number` but found `'x'`"

[[test.results]]
error = "Incorrect arguments for function math::max(). Argument 1 was the wrong type. Expected `number` but found `'y'`"

[[test.results]]
error = "Incorrect arguments for function math::min(). Argument 1 was the wrong type. Expected `number` but found `'y'`"

[[test.results]]
value = "[{ m: 3 }]"

[[test.results]]
value = "[{ m: 3 }]"

[[test.results]]
value = "[{ m: Infinity }]"

[[test.results]]
value = "[{ m: -Infinity }]"

[[test.results]]
value = "false"

[[test.results]]
value = "[{ m: 12 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "1000.5f"

[[test.results]]
error = "Incorrect arguments for function math::min(). Argument 1 was the wrong type. Expected `number` but found `'x'`"

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
	-- `5` and `5f` equal at the maximum, in both orders of record id, and at the minimum.
	CREATE b1:1 SET n = 5 RETURN NONE;
	CREATE b1:2 SET n = 5f RETURN NONE;
	CREATE b1:3 SET n = 1 RETURN NONE;
	CREATE b2:1 SET n = 5f RETURN NONE;
	CREATE b2:2 SET n = 5 RETURN NONE;
	CREATE b2:3 SET n = 1 RETURN NONE;
	CREATE b3:1 SET n = 5 RETURN NONE;
	CREATE b3:2 SET n = 5f RETURN NONE;
	CREATE b3:3 SET n = 9 RETURN NONE;
	CREATE b4:1 SET n = 5f RETURN NONE;
	CREATE b4:2 SET n = 5 RETURN NONE;
	CREATE b4:3 SET n = 9 RETURN NONE;
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
	-- Infinities and a float equal to an integer at the minimum.
	CREATE j:1 SET n = 7 RETURN NONE;
	CREATE j:2 SET n = -1f RETURN NONE;
	CREATE j:3 SET n = -1 RETURN NONE;
	CREATE j:4 SET n = math::inf RETURN NONE;
	CREATE j:5 SET n = -math::inf RETURN NONE;
	-- A decimal and a float equal to the integer at the maximum.
	CREATE k:1 SET n = 12 RETURN NONE;
	CREATE k:2 SET n = 12dec RETURN NONE;
	CREATE k:3 SET n = 12f RETURN NONE;
	CREATE k:4 SET n = 2 RETURN NONE;
	RETURN 'OK';
};

-- The declarations, which the rows above predate.
DEFINE FIELD n ON a TYPE int;
DEFINE INDEX n ON a FIELDS n;
DEFINE FIELD n ON b1 TYPE int;
DEFINE INDEX n ON b1 FIELDS n;
DEFINE FIELD n ON b2 TYPE int;
DEFINE INDEX n ON b2 FIELDS n;
DEFINE FIELD n ON b3 TYPE int;
DEFINE INDEX n ON b3 FIELDS n;
DEFINE FIELD n ON b4 TYPE int;
DEFINE INDEX n ON b4 FIELDS n;
DEFINE FIELD n ON c TYPE int;
DEFINE INDEX n ON c FIELDS n;
DEFINE FIELD n ON d TYPE int;
DEFINE INDEX n ON d FIELDS n;
DEFINE FIELD n ON e TYPE int;
DEFINE INDEX n ON e FIELDS n;
DEFINE FIELD n ON f TYPE int;
DEFINE INDEX n ON f FIELDS n;
DEFINE FIELD n ON g TYPE int;
DEFINE INDEX n ON g FIELDS n;
DEFINE FIELD n ON h TYPE int;
DEFINE INDEX n ON h FIELDS n;
DEFINE FIELD n ON i TYPE int;
DEFINE INDEX n ON i FIELDS n;
DEFINE FIELD n ON j TYPE int;
DEFINE INDEX n ON j FIELDS n;
DEFINE FIELD n ON k TYPE int;
DEFINE INDEX n ON k FIELDS n;

-- Text, a missing value and a float above every integer, beside integers.
SELECT math::max(n) AS m FROM a GROUP ALL;
SELECT math::min(n) AS m FROM a GROUP ALL;
-- Equal values at the extreme answer the first in scan order, with its kind.
SELECT math::max(n) AS m FROM b1 GROUP ALL;
SELECT math::max(n) AS m FROM b2 GROUP ALL;
SELECT math::min(n) AS m FROM b3 GROUP ALL;
SELECT math::min(n) AS m FROM b4 GROUP ALL;
RETURN type::is_int((SELECT VALUE math::max(n) FROM b1 GROUP ALL)[0]);
RETURN type::is_float((SELECT VALUE math::max(n) FROM b2 GROUP ALL)[0]);
RETURN type::is_int((SELECT VALUE math::min(n) FROM b3 GROUP ALL)[0]);
RETURN type::is_float((SELECT VALUE math::min(n) FROM b4 GROUP ALL)[0]);
SELECT math::min(n) AS m FROM b1 GROUP ALL;
SELECT math::max(n) AS m FROM b3 GROUP ALL;
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
-- Infinities, and a float equal to an integer at the minimum.
SELECT math::max(n) AS m FROM j GROUP ALL;
SELECT math::min(n) AS m FROM j GROUP ALL;
RETURN type::is_int((SELECT VALUE math::min(n) FROM j GROUP ALL)[0]);
-- A decimal and a float equal to the integer at the maximum.
SELECT math::max(n) AS m FROM k GROUP ALL;
RETURN type::is_int((SELECT VALUE math::max(n) FROM k GROUP ALL)[0]);
RETURN type::is_decimal((SELECT VALUE math::max(n) FROM k GROUP ALL)[0]);
RETURN type::is_float((SELECT VALUE math::max(n) FROM k GROUP ALL)[0]);
-- Value position and paging of the same rows.
RETURN (SELECT VALUE math::max(n) FROM a GROUP ALL)[0];
SELECT id, (SELECT VALUE math::min(n) FROM c GROUP ALL)[0] AS m FROM d ORDER BY id;
SELECT math::max(n) AS m FROM a GROUP ALL LIMIT 1;
