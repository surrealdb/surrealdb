/**
[env]
namespace = true
database = true
timeout = 20000

[test]
reason = """
	The extreme of an int field under a `UNIQUE` index over rows whose record ids are arrays and
	objects holding numbers of different kinds, which an index of the ids cannot tell apart, in both
	orders of creation, beside scalar ids, and over rows written as floats and decimals before the
	declaration. A unique index holds one row per number, so the answer is the row that holds the
	extreme whatever its id. Covers a maximum and a minimum, the kind of the answer, and the
	statement in value position.
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
value = "[{ m: 9 }]"

[[test.results]]
value = "[{ m: 5 }]"

[[test.results]]
value = "[{ m: 9 }]"

[[test.results]]
value = "[{ m: 5 }]"

[[test.results]]
value = "[{ m: 11 }]"

[[test.results]]
value = "[{ m: 3 }]"

[[test.results]]
value = "[{ m: 100 }]"

[[test.results]]
value = "[{ m: -4 }]"

[[test.results]]
value = "[{ m: 9f }]"

[[test.results]]
value = "[{ m: 1 }]"

[[test.results]]
value = "[{ m: 12 }]"

[[test.results]]
value = "[{ m: 6 }]"

[[test.results]]
value = "'int'"

[[test.results]]
value = "'int'"

[[test.results]]
value = "'int'"

[[test.results]]
value = "'int'"

[[test.results]]
value = "'int'"

[[test.results]]
value = "'int'"

[[test.results]]
value = "'int'"

[[test.results]]
value = "'int'"

[[test.results]]
value = "'float'"

[[test.results]]
value = "'int'"

[[test.results]]
value = "'int'"

[[test.results]]
value = "'int'"

[[test.results]]
value = "[a:[1dec]]"

[[test.results]]
value = "[a:[1]]"

[[test.results]]
value = "[b:[1dec]]"

[[test.results]]
value = "[b:[1]]"

[[test.results]]
value = "[c:{ a: 1dec }]"

[[test.results]]
value = "[c:{ a: 1f }]"

[[test.results]]
value = "[d:[1f]]"

[[test.results]]
value = "[d:k]"

[[test.results]]
value = "[e:[1]]"

[[test.results]]
value = "[e:[1f]]"

[[test.results]]
value = "[f:[2, 1dec]]"

[[test.results]]
value = "[f:[2, 1]]"
*/
{
	CREATE a:[1] SET n = 5;
	CREATE a:[1dec] SET n = 9;
	CREATE b:[1dec] SET n = 9;
	CREATE b:[1] SET n = 5;
	CREATE c:{a: 1} SET n = 7;
	CREATE c:{a: 1f} SET n = 3;
	CREATE c:{a: 1dec} SET n = 11;
	CREATE d:7 SET n = 1;
	CREATE d:[1f] SET n = 100;
	CREATE d:[1dec] SET n = 50;
	CREATE d:k SET n = -4;
	CREATE e:[1] SET n = 9f;
	CREATE e:[1dec] SET n = 8dec;
	CREATE e:[1f] SET n = 1;
	CREATE f:[2, 1] SET n = 6;
	CREATE f:[2, 1dec] SET n = 12;
	CREATE f:[1, 9] SET n = 6.5f;
	RETURN 'OK';
};
DEFINE FIELD n ON a TYPE int; DEFINE INDEX n ON a FIELDS n UNIQUE;
DEFINE FIELD n ON b TYPE int; DEFINE INDEX n ON b FIELDS n UNIQUE;
DEFINE FIELD n ON c TYPE int; DEFINE INDEX n ON c FIELDS n UNIQUE;
DEFINE FIELD n ON d TYPE int; DEFINE INDEX n ON d FIELDS n UNIQUE;
DEFINE FIELD n ON e TYPE int; DEFINE INDEX n ON e FIELDS n UNIQUE;
DEFINE FIELD n ON f TYPE int; DEFINE INDEX n ON f FIELDS n UNIQUE;
SELECT math::max(n) AS m FROM a GROUP ALL;
SELECT math::min(n) AS m FROM a GROUP ALL;
SELECT math::max(n) AS m FROM b GROUP ALL;
SELECT math::min(n) AS m FROM b GROUP ALL;
SELECT math::max(n) AS m FROM c GROUP ALL;
SELECT math::min(n) AS m FROM c GROUP ALL;
SELECT math::max(n) AS m FROM d GROUP ALL;
SELECT math::min(n) AS m FROM d GROUP ALL;
SELECT math::max(n) AS m FROM e GROUP ALL;
SELECT math::min(n) AS m FROM e GROUP ALL;
SELECT math::max(n) AS m FROM f GROUP ALL;
SELECT math::min(n) AS m FROM f GROUP ALL;
RETURN type::of((SELECT VALUE math::max(n) FROM a GROUP ALL)[0]);
RETURN type::of((SELECT VALUE math::min(n) FROM a GROUP ALL)[0]);
RETURN type::of((SELECT VALUE math::max(n) FROM b GROUP ALL)[0]);
RETURN type::of((SELECT VALUE math::min(n) FROM b GROUP ALL)[0]);
RETURN type::of((SELECT VALUE math::max(n) FROM c GROUP ALL)[0]);
RETURN type::of((SELECT VALUE math::min(n) FROM c GROUP ALL)[0]);
RETURN type::of((SELECT VALUE math::max(n) FROM d GROUP ALL)[0]);
RETURN type::of((SELECT VALUE math::min(n) FROM d GROUP ALL)[0]);
RETURN type::of((SELECT VALUE math::max(n) FROM e GROUP ALL)[0]);
RETURN type::of((SELECT VALUE math::min(n) FROM e GROUP ALL)[0]);
RETURN type::of((SELECT VALUE math::max(n) FROM f GROUP ALL)[0]);
RETURN type::of((SELECT VALUE math::min(n) FROM f GROUP ALL)[0]);
RETURN (SELECT VALUE id FROM a WHERE n = (SELECT VALUE math::max(n) FROM a GROUP ALL)[0]);
RETURN (SELECT VALUE id FROM a WHERE n = (SELECT VALUE math::min(n) FROM a GROUP ALL)[0]);
RETURN (SELECT VALUE id FROM b WHERE n = (SELECT VALUE math::max(n) FROM b GROUP ALL)[0]);
RETURN (SELECT VALUE id FROM b WHERE n = (SELECT VALUE math::min(n) FROM b GROUP ALL)[0]);
RETURN (SELECT VALUE id FROM c WHERE n = (SELECT VALUE math::max(n) FROM c GROUP ALL)[0]);
RETURN (SELECT VALUE id FROM c WHERE n = (SELECT VALUE math::min(n) FROM c GROUP ALL)[0]);
RETURN (SELECT VALUE id FROM d WHERE n = (SELECT VALUE math::max(n) FROM d GROUP ALL)[0]);
RETURN (SELECT VALUE id FROM d WHERE n = (SELECT VALUE math::min(n) FROM d GROUP ALL)[0]);
RETURN (SELECT VALUE id FROM e WHERE n = (SELECT VALUE math::max(n) FROM e GROUP ALL)[0]);
RETURN (SELECT VALUE id FROM e WHERE n = (SELECT VALUE math::min(n) FROM e GROUP ALL)[0]);
RETURN (SELECT VALUE id FROM f WHERE n = (SELECT VALUE math::max(n) FROM f GROUP ALL)[0]);
RETURN (SELECT VALUE id FROM f WHERE n = (SELECT VALUE math::min(n) FROM f GROUP ALL)[0]);
