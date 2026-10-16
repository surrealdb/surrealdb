/**
[env]
namespace = true
database = true
timeout = 20000

[test]
reason = """
	The extreme of an indexed `int` field whose rows were written before the declaration, held by
	several rows whose record ids are arrays or objects. Equal numbers of different kinds tie, and
	the answer, with its kind, is the one of the first of them in the order a scan of the table
	delivers its rows. That order is the order of the bytes a record is addressed by, which carry
	the kind of a number inside an array or an object, so `[1]` stands before `[1dec]` although the
	two ids compare equal as values. Covers ids of an array and of an object holding one number
	in each kind, both orders of the kinds, three kinds at once, a scalar id beside them, a unique
	maximum whose row is one of two that an index cannot tell apart, a table of many rows, a minimum
	and a maximum, and the statement in value position. The index is not unique, so the plan does
	not read it for an extreme and each statement is answered by the fold over the rows.
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
value = "[{ m: 5 }]"

[[test.results]]
value = "[{ m: 5 }]"

[[test.results]]
value = "[{ m: 5f }]"

[[test.results]]
value = "[{ m: 5f }]"

[[test.results]]
value = "[{ m: 5 }]"

[[test.results]]
value = "[{ m: 5 }]"

[[test.results]]
value = "[{ m: 5dec }]"

[[test.results]]
value = "[{ m: 5dec }]"

[[test.results]]
value = "[{ m: 5dec }]"

[[test.results]]
value = "[{ m: 5dec }]"

[[test.results]]
value = "[{ m: 5dec }]"

[[test.results]]
value = "[{ m: 4 }]"

[[test.results]]
value = "[{ m: 5dec }]"

[[test.results]]
value = "[{ m: 5dec }]"

[[test.results]]
value = "[{ m: 5f }]"

[[test.results]]
value = "[{ m: 3 }]"

[[test.results]]
value = "[{ m: 5 }]"

[[test.results]]
value = "[{ m: 1 }]"

[[test.results]]
value = "[{ m: 200 }]"

[[test.results]]
value = "[{ m: 1 }]"

[[test.results]]
value = "'int'"

[[test.results]]
value = "'int'"

[[test.results]]
value = "'float'"

[[test.results]]
value = "'float'"

[[test.results]]
value = "'int'"

[[test.results]]
value = "'int'"

[[test.results]]
value = "'decimal'"

[[test.results]]
value = "'decimal'"

[[test.results]]
value = "'decimal'"

[[test.results]]
value = "'decimal'"

[[test.results]]
value = "'decimal'"

[[test.results]]
value = "'int'"

[[test.results]]
value = "'decimal'"

[[test.results]]
value = "'decimal'"

[[test.results]]
value = "'float'"

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

*/
{
	CREATE a:[1] SET n = 5;
	CREATE a:[1dec] SET n = 5f;
	CREATE b:[1] SET n = 5f;
	CREATE b:[1dec] SET n = 5;
	CREATE c:{a: 1} SET n = 5;
	CREATE c:{a: 1f} SET n = 5dec;
	CREATE c:{a: 1dec} SET n = 5f;
	CREATE d:[1] SET n = 5dec;
	CREATE d:[1f] SET n = 5;
	CREATE d:[1dec] SET n = 5f;
	CREATE e:[1dec] SET n = 5;
	CREATE e:[1] SET n = 5dec;
	CREATE e:[1f] SET n = 5f;
	CREATE f:[1] SET n = 5;
	CREATE f:[1dec] SET n = 5f;
	CREATE f:7 SET n = 5dec;
	CREATE f:k SET n = 4;
	CREATE g:[2, 1] SET n = 5f;
	CREATE g:[2, 1dec] SET n = 5;
	CREATE g:[1, 9] SET n = 5dec;
	CREATE h:[1] SET n = 5f;
	CREATE h:[1dec] SET n = 5;
	CREATE h:[1f] SET n = 3;
	CREATE h:[2] SET n = 5dec;
	CREATE i:[1] SET n = 5;
	CREATE i:[1dec] SET n = 5f;
	CREATE i:3 SET n = 1;
	FOR $k IN 1..101 { CREATE type::record('j', $k) SET n = $k; };
	CREATE j:[1] SET n = 200;
	CREATE j:[1dec] SET n = 200f;
	RETURN 'OK';
};
DEFINE FIELD n ON a TYPE int; DEFINE INDEX n ON a FIELDS n;
DEFINE FIELD n ON b TYPE int; DEFINE INDEX n ON b FIELDS n;
DEFINE FIELD n ON c TYPE int; DEFINE INDEX n ON c FIELDS n;
DEFINE FIELD n ON d TYPE int; DEFINE INDEX n ON d FIELDS n;
DEFINE FIELD n ON e TYPE int; DEFINE INDEX n ON e FIELDS n;
DEFINE FIELD n ON f TYPE int; DEFINE INDEX n ON f FIELDS n;
DEFINE FIELD n ON g TYPE int; DEFINE INDEX n ON g FIELDS n;
DEFINE FIELD n ON h TYPE int; DEFINE INDEX n ON h FIELDS n;
DEFINE FIELD n ON i TYPE int; DEFINE INDEX n ON i FIELDS n;
DEFINE FIELD n ON j TYPE int; DEFINE INDEX n ON j FIELDS n;
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
SELECT math::max(n) AS m FROM g GROUP ALL;
SELECT math::min(n) AS m FROM g GROUP ALL;
SELECT math::max(n) AS m FROM h GROUP ALL;
SELECT math::min(n) AS m FROM h GROUP ALL;
SELECT math::max(n) AS m FROM i GROUP ALL;
SELECT math::min(n) AS m FROM i GROUP ALL;
SELECT math::max(n) AS m FROM j GROUP ALL;
SELECT math::min(n) AS m FROM j GROUP ALL;
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
RETURN type::of((SELECT VALUE math::max(n) FROM g GROUP ALL)[0]);
RETURN type::of((SELECT VALUE math::min(n) FROM g GROUP ALL)[0]);
RETURN type::of((SELECT VALUE math::max(n) FROM h GROUP ALL)[0]);
RETURN type::of((SELECT VALUE math::min(n) FROM h GROUP ALL)[0]);
RETURN type::of((SELECT VALUE math::max(n) FROM i GROUP ALL)[0]);
RETURN type::of((SELECT VALUE math::min(n) FROM i GROUP ALL)[0]);
RETURN type::of((SELECT VALUE math::max(n) FROM j GROUP ALL)[0]);
RETURN type::of((SELECT VALUE math::min(n) FROM j GROUP ALL)[0]);
