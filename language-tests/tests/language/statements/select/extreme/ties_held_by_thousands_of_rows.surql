/**
[env]
namespace = true
database = true
timeout = 60000

[test]
reason = """
	The extreme of an indexed `int` field held by thousands of rows at once, the rows written
	before the field was declared so that equal numbers may be of different kinds. The index is not
	unique, so the plan does not read it for an extreme: the answer is the fold over the rows, which
	keeps the first of the equal numbers in record id order, with its kind, however many rows hold
	the extreme and wherever in the table the first of them stands.
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
value = "[{ m: 500f }]"

[[test.results]]
value = "[{ m: 500f }]"

[[test.results]]
value = "[{ m: 500 }]"

[[test.results]]
value = "[{ m: 500 }]"

[[test.results]]
value = "[{ m: 500dec }]"

[[test.results]]
value = "[{ m: 0 }]"

[[test.results]]
value = "[{ m: 500dec }]"

[[test.results]]
value = "[{ m: 500dec }]"

[[test.results]]
value = "[{ m: -500f }]"

[[test.results]]
value = "[{ m: -500f }]"

[[test.results]]
value = "[{ m: -500 }]"

[[test.results]]
value = "[{ m: -500 }]"

[[test.results]]
value = "[{ m: 7 }]"

[[test.results]]
value = "[{ m: 7 }]"

[[test.results]]
value = "500f"

[[test.results]]
value = "500dec"

[[test.results]]
value = "-500f"

[[test.results]]
value = "7"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ m: 500f }]"
*/
{
	FOR $i IN 1..3001 { CREATE type::record('a', $i) SET n = IF $i = 1 { 500f } ELSE IF $i % 7 = 0 { 500dec } ELSE { 500 }; };
	FOR $i IN 1..3001 { CREATE type::record('b', $i) SET n = IF $i = 3000 { 500f } ELSE { 500 }; };
	FOR $i IN 1..3001 { CREATE type::record('c', $i) SET n = IF $i = 1 { 500dec } ELSE IF $i < 70 { 500 } ELSE { $i % 400 }; };
	FOR $i IN 1..3001 { CREATE type::record('d', $i) SET n = IF $i = 2000 { 500f } ELSE IF $i < 1500 { 500dec } ELSE { 500 }; };
	FOR $i IN 1..3001 { CREATE type::record('e', $i) SET n = IF $i = 1 { -500f } ELSE IF $i % 7 = 0 { -500dec } ELSE { -500 }; };
	FOR $i IN 1..3001 { CREATE type::record('f', $i) SET n = IF $i = 3000 { -500f } ELSE { -500 }; };
	FOR $i IN 1..3001 { CREATE type::record('g', $i) SET n = IF $i < 5 { 7 } ELSE IF $i < 100 { 7f } ELSE { 7dec }; };
	RETURN 'OK';
};
DEFINE FIELD n ON a TYPE int; DEFINE INDEX n ON a FIELDS n;
DEFINE FIELD n ON b TYPE int; DEFINE INDEX n ON b FIELDS n;
DEFINE FIELD n ON c TYPE int; DEFINE INDEX n ON c FIELDS n;
DEFINE FIELD n ON d TYPE int; DEFINE INDEX n ON d FIELDS n;
DEFINE FIELD n ON e TYPE int; DEFINE INDEX n ON e FIELDS n;
DEFINE FIELD n ON f TYPE int; DEFINE INDEX n ON f FIELDS n;
DEFINE FIELD n ON g TYPE int; DEFINE INDEX n ON g FIELDS n;
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
RETURN (SELECT VALUE math::max(n) FROM a GROUP ALL)[0];
RETURN (SELECT VALUE math::max(n) FROM c GROUP ALL)[0];
RETURN (SELECT VALUE math::min(n) FROM e GROUP ALL)[0];
RETURN (SELECT VALUE math::max(n) FROM g GROUP ALL)[0];
RETURN type::is_float((SELECT VALUE math::max(n) FROM a GROUP ALL)[0]);
RETURN type::is_decimal((SELECT VALUE math::max(n) FROM c GROUP ALL)[0]);
RETURN type::is_float((SELECT VALUE math::min(n) FROM e GROUP ALL)[0]);
SELECT math::max(n) AS m FROM a GROUP ALL LIMIT 1 START 0;
