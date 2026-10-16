/**
[env]
namespace = true
database = true
timeout = 20000

[test]
reason = """
	The extreme of a field that is not declared exactly `int`, or that the plan cannot read as
	stored, under a `UNIQUE` index over it: the one index the plan answers an extreme from, so that
	the kind of the field is what keeps each of these statements on the fold over the rows. A
	float, a decimal, a field of several kinds, a `NONE`, NaN, a table of text that no number is
	among, a field a `VALUE` clause assigns, an array, a composite index and a unique index beside a
	non-unique one.
"""

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ m: 9 }]"

[[test.results]]
value = "[{ m: 3 }]"

[[test.results]]
value = "[{ m: 9 }]"

[[test.results]]
value = "[{ m: 1 }]"

[[test.results]]
value = "[{ m: 9f }]"

[[test.results]]
value = "[{ m: 1f }]"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ m: 7.25f }]"

[[test.results]]
value = "[{ m: -3.5f }]"

[[test.results]]
value = "[{ m: 7.25dec }]"

[[test.results]]
value = "[{ m: 1.5dec }]"

[[test.results]]
value = "[{ m: 11 }]"

[[test.results]]
value = "[{ m: 4 }]"

[[test.results]]
value = "[{ m: NaN }]"

[[test.results]]
value = "[{ m: 2.5f }]"

[[test.results]]
error = "Incorrect arguments for function math::max(). Argument 1 was the wrong type. Expected `number` but found `'x'`"

[[test.results]]
error = "Incorrect arguments for function math::min(). Argument 1 was the wrong type. Expected `number` but found `'x'`"

[[test.results]]
value = "[{ m: 7 }]"

[[test.results]]
value = "[{ m: 2 }]"

[[test.results]]
value = "[{ m: 8 }]"

[[test.results]]
value = "[{ m: 4 }]"

[[test.results]]
value = "[{ m: 8 }]"

[[test.results]]
value = "[{ m: 4 }]"

[[test.results]]
error = "Incorrect arguments for function math::max(). Argument 1 was the wrong type. Expected `number` but found `[1, 5]`"

[[test.results]]
error = "Incorrect arguments for function math::min(). Argument 1 was the wrong type. Expected `number` but found `[1, 5]`"

[[test.results]]
value = "9"

[[test.results]]
value = "2.5f"

[[test.results]]
value = "11"
*/
{
	DEFINE TABLE opt SCHEMAFULL;
	DEFINE FIELD n ON opt TYPE option<int>;
	DEFINE INDEX n ON opt FIELDS n UNIQUE;
	CREATE opt:1 SET n = 3 RETURN NONE;
	CREATE opt:2 SET n = 9 RETURN NONE;
	CREATE opt:3 RETURN NONE;
	DEFINE TABLE num SCHEMAFULL;
	DEFINE FIELD n ON num TYPE number;
	DEFINE INDEX n ON num FIELDS n UNIQUE;
	CREATE num:1 SET n = 9 RETURN NONE;
	CREATE num:2 SET n = 8f RETURN NONE;
	CREATE num:3 SET n = 1 RETURN NONE;
	CREATE num:4 SET n = 2f RETURN NONE;
	DEFINE TABLE numrev SCHEMAFULL;
	DEFINE FIELD n ON numrev TYPE number;
	DEFINE INDEX n ON numrev FIELDS n UNIQUE;
	CREATE numrev:1 SET n = 9f RETURN NONE;
	CREATE numrev:2 SET n = 8 RETURN NONE;
	CREATE numrev:3 SET n = 1f RETURN NONE;
	CREATE numrev:4 SET n = 2 RETURN NONE;
	DEFINE TABLE flt SCHEMAFULL;
	DEFINE FIELD n ON flt TYPE float;
	DEFINE INDEX n ON flt FIELDS n UNIQUE;
	CREATE flt:1 SET n = 1.5 RETURN NONE;
	CREATE flt:2 SET n = 7.25 RETURN NONE;
	CREATE flt:3 SET n = -3.5 RETURN NONE;
	DEFINE TABLE dec SCHEMAFULL;
	DEFINE FIELD n ON dec TYPE decimal;
	DEFINE INDEX n ON dec FIELDS n UNIQUE;
	CREATE dec:1 SET n = 1.5dec RETURN NONE;
	CREATE dec:2 SET n = 7.25dec RETURN NONE;
	DEFINE TABLE untyped SCHEMALESS;
	DEFINE INDEX n ON untyped FIELDS n UNIQUE;
	CREATE untyped:1 SET n = 4 RETURN NONE;
	CREATE untyped:2 SET n = 'x' RETURN NONE;
	CREATE untyped:3 SET n = 11 RETURN NONE;
	DEFINE TABLE nan SCHEMAFULL;
	DEFINE FIELD n ON nan TYPE float;
	DEFINE INDEX n ON nan FIELDS n UNIQUE;
	CREATE nan:1 SET n = 2.5 RETURN NONE;
	CREATE nan:2 SET n = NaN RETURN NONE;
	CREATE nan:3 SET n = 7.5 RETURN NONE;
	DEFINE TABLE words SCHEMAFULL;
	DEFINE FIELD n ON words TYPE number | string;
	DEFINE INDEX n ON words FIELDS n UNIQUE;
	CREATE words:1 SET n = 'x' RETURN NONE;
	CREATE words:2 SET n = 'y' RETURN NONE;
	DEFINE TABLE assigned SCHEMAFULL;
	DEFINE FIELD n ON assigned TYPE int VALUE $value + 1;
	DEFINE INDEX n ON assigned FIELDS n UNIQUE;
	CREATE assigned:1 SET n = 1 RETURN NONE;
	CREATE assigned:2 SET n = 6 RETURN NONE;
	DEFINE TABLE pair SCHEMAFULL;
	DEFINE FIELD n ON pair TYPE int;
	DEFINE FIELD m ON pair TYPE int;
	DEFINE INDEX n_m ON pair FIELDS n, m UNIQUE;
	CREATE pair:1 SET n = 4, m = 9 RETURN NONE;
	CREATE pair:2 SET n = 8, m = 3 RETURN NONE;
	CREATE pair:3 SET n = 8, m = 1 RETURN NONE;
	DEFINE TABLE both SCHEMAFULL;
	DEFINE FIELD n ON both TYPE int;
	DEFINE INDEX n ON both FIELDS n UNIQUE;
	DEFINE INDEX n_dup ON both FIELDS n;
	CREATE both:1 SET n = 4 RETURN NONE;
	CREATE both:2 SET n = 8 RETURN NONE;
	DEFINE TABLE arr SCHEMAFULL;
	DEFINE FIELD n ON arr TYPE array<int>;
	DEFINE INDEX n ON arr FIELDS n UNIQUE;
	CREATE arr:1 SET n = [1, 5] RETURN NONE;
	CREATE arr:2 SET n = [9, 2] RETURN NONE;
	RETURN 'OK';
};
SELECT math::max(n) AS m FROM opt GROUP ALL;
SELECT math::min(n) AS m FROM opt GROUP ALL;
SELECT math::max(n) AS m FROM num GROUP ALL;
SELECT math::min(n) AS m FROM num GROUP ALL;
SELECT math::max(n) AS m FROM numrev GROUP ALL;
SELECT math::min(n) AS m FROM numrev GROUP ALL;
RETURN type::is_int((SELECT VALUE math::max(n) FROM num GROUP ALL)[0]);
RETURN type::is_float((SELECT VALUE math::max(n) FROM numrev GROUP ALL)[0]);
RETURN type::is_int((SELECT VALUE math::min(n) FROM num GROUP ALL)[0]);
RETURN type::is_float((SELECT VALUE math::min(n) FROM numrev GROUP ALL)[0]);
SELECT math::max(n) AS m FROM flt GROUP ALL;
SELECT math::min(n) AS m FROM flt GROUP ALL;
SELECT math::max(n) AS m FROM dec GROUP ALL;
SELECT math::min(n) AS m FROM dec GROUP ALL;
SELECT math::max(n) AS m FROM untyped GROUP ALL;
SELECT math::min(n) AS m FROM untyped GROUP ALL;
SELECT math::max(n) AS m FROM nan GROUP ALL;
SELECT math::min(n) AS m FROM nan GROUP ALL;
SELECT math::max(n) AS m FROM words GROUP ALL;
SELECT math::min(n) AS m FROM words GROUP ALL;
SELECT math::max(n) AS m FROM assigned GROUP ALL;
SELECT math::min(n) AS m FROM assigned GROUP ALL;
SELECT math::max(n) AS m FROM pair GROUP ALL;
SELECT math::min(n) AS m FROM pair GROUP ALL;
SELECT math::max(n) AS m FROM both GROUP ALL;
SELECT math::min(n) AS m FROM both GROUP ALL;
SELECT math::max(n) AS m FROM arr GROUP ALL;
SELECT math::min(n) AS m FROM arr GROUP ALL;
RETURN (SELECT VALUE math::max(n) FROM opt GROUP ALL)[0];
RETURN (SELECT VALUE math::min(n) FROM nan GROUP ALL)[0];
RETURN (SELECT VALUE math::max(n) FROM untyped GROUP ALL)[0];
