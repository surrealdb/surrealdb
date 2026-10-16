/**
[env]
namespace = true
database = true

[test]
reason = """
	The extreme of an expression, of a filtered read, of a statement that reads more than the
	extreme and of a source that is not a whole table answers as the fold over the rows does.
"""

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ m: 4 }]"

[[test.results]]
value = "[{ m: 8 }]"

[[test.results]]
value = "[{ a: 8, b: 4 }]"

[[test.results]]
value = "[{ a: 8, c: 2 }]"

[[test.results]]
value = "[{ a: 4, c: 2 }]"

[[test.results]]
value = "[{ m: -Infinity }]"

[[test.results]]
value = "[{ m: 9 }]"

[[test.results]]
value = "[{ m: 4, n: 4 }, { m: 8, n: 8 }]"

[[test.results]]
value = "[{ m: 8 }]"

[[test.results]]
value = "[{ m: 8 }]"

[[test.results]]
value = "[{ m: 8 }]"

[[test.results]]
value = "[{ m: 8 }]"

[[test.results]]
value = "[{ m: 8 }]"

[[test.results]]
value = "[{ m: 8 }]"

[[test.results]]
value = "[{ m: 8 }]"

[[test.results]]
value = "[{ m: 8 }]"

[[test.results]]
value = "[{ m: 8 }]"

[[test.results]]
error = "The table 'nothere' does not exist"
*/
{
	-- Tables the statements read.
	DEFINE TABLE bare SCHEMAFULL;
	DEFINE FIELD n ON bare TYPE int;
	CREATE bare:1 SET n = 4 RETURN NONE;
	CREATE bare:2 SET n = 8 RETURN NONE;
	DEFINE TABLE uniq SCHEMAFULL;
	DEFINE FIELD n ON uniq TYPE int;
	DEFINE INDEX n ON uniq FIELDS n UNIQUE;
	CREATE uniq:1 SET n = 4 RETURN NONE;
	CREATE uniq:2 SET n = 8 RETURN NONE;
	DEFINE TABLE obj SCHEMAFULL;
	DEFINE FIELD n ON obj TYPE int;
	DEFINE FIELD o ON obj TYPE object FLEXIBLE;
	DEFINE INDEX n ON obj FIELDS n;
	CREATE obj:1 SET n = 4, o = { x: 3 } RETURN NONE;
	CREATE obj:2 SET n = 8, o = { x: 9 } RETURN NONE;
	RETURN 'OK';
};

-- Statements that read more than the extreme of the field.
SELECT math::max(n) AS m FROM uniq WHERE n < 8 GROUP ALL;
SELECT math::min(n) AS m FROM uniq WHERE n > 4 GROUP ALL;
SELECT math::max(n) AS a, math::min(n) AS b FROM uniq GROUP ALL;
SELECT math::max(n) AS a, count() AS c FROM uniq GROUP ALL;
SELECT count() AS c, math::min(n) AS a FROM uniq GROUP ALL;
SELECT math::max(n.x) AS m FROM uniq GROUP ALL;
SELECT math::max(o.x) AS m FROM obj GROUP ALL;
SELECT n, math::max(n) AS m FROM uniq GROUP BY n;
SELECT math::max(n) AS m FROM uniq WITH NOINDEX GROUP ALL;
SELECT math::max(n) AS m FROM uniq WITH INDEX n GROUP ALL;
SELECT math::max(n) AS m FROM uniq GROUP ALL ORDER BY m;
SELECT math::max(n) AS m FROM uniq GROUP ALL FETCH m;
SELECT math::max(<int> n) AS m FROM uniq GROUP ALL;
SELECT math::max(n) AS m FROM uniq:1..3 GROUP ALL;
SELECT math::max(n) AS m FROM [uniq:1, uniq:2] GROUP ALL;
SELECT math::max(n) AS m FROM uniq:2 GROUP ALL;
SELECT math::max(n) AS m FROM uniq, bare GROUP ALL;
SELECT math::max(n) AS m FROM nothere GROUP ALL;
