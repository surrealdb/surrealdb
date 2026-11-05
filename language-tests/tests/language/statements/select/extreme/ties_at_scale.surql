/**
[env]
namespace = true
database = true
timeout = 60000

[test]
reason = """
	The extreme of an indexed `int` field held by many rows at once, the rows written before the
	field was declared so that equal numbers may be of different kinds. The first of the equal
	numbers in record id order, with its kind, is the answer whatever the number of rows that tie,
	from a few to hundreds. Rows of an interleaved mix of kinds and rows whose record ids are not in
	the order of their values are covered. The index is not unique, so the plan does not read it
	for an extreme and each statement is answered by the fold over the rows.
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
value = "[{ m: 7 }]"

[[test.results]]
value = "[{ m: 1 }]"

[[test.results]]
value = "[{ m: 7f }]"

[[test.results]]
value = "[{ m: 1 }]"

[[test.results]]
value = "[{ m: 9dec }]"

[[test.results]]
value = "[{ m: 2 }]"

[[test.results]]
value = "[{ m: 9dec }]"

[[test.results]]
value = "[{ m: 2 }]"

[[test.results]]
value = "[{ m: 9dec }]"

[[test.results]]
value = "[{ m: 2 }]"

[[test.results]]
value = "[{ m: 999 }]"

[[test.results]]
value = "[{ m: 800 }]"

[[test.results]]
value = "[{ m: 200 }]"

[[test.results]]
value = "[{ m: 1 }]"

[[test.results]]
value = "[{ m: 9 }]"

[[test.results]]
value = "[{ m: 0f }]"

[[test.results]]
value = "[{ m: 40 }]"

[[test.results]]
value = "[{ m: -41 }]"

[[test.results]]
value = "[{ m: 8f }]"

[[test.results]]
value = "[{ m: 1 }]"

[[test.results]]
value = "7"

[[test.results]]
value = "1"

[[test.results]]
value = "7f"

[[test.results]]
value = "1"

[[test.results]]
value = "9dec"

[[test.results]]
value = "2"

[[test.results]]
value = "9dec"

[[test.results]]
value = "2"

[[test.results]]
value = "9dec"

[[test.results]]
value = "2"

[[test.results]]
value = "999"

[[test.results]]
value = "800"

[[test.results]]
value = "200"

[[test.results]]
value = "1"

[[test.results]]
value = "9"

[[test.results]]
value = "0f"

[[test.results]]
value = "40"

[[test.results]]
value = "-41"

[[test.results]]
value = "8f"

[[test.results]]
value = "1"

[[test.results]]
value = "[{ m: 7 }]"

[[test.results]]
value = "9dec"

[[test.results]]
value = "[{ hi: 7f, id: w10:a }, { hi: 7f, id: w10:b }, { hi: 7f, id: w10:c }, { hi: 7f, id: w10:d }, { hi: 7f, id: w10:e }]"
*/
{
	FOR $i IN 1..41 { CREATE type::record('w1', $i) SET n = IF $i % 3 = 1 { 7 } ELSE IF $i % 3 = 2 { 7dec } ELSE { 7f } RETURN NONE; };
	FOR $i IN 100..105 { CREATE type::record('w1', $i) SET n = $i - 99 RETURN NONE; };
	FOR $i IN 1..41 { CREATE type::record('w2', $i) SET n = IF $i % 3 = 1 { 7f } ELSE IF $i % 3 = 2 { 7 } ELSE { 7dec } RETURN NONE; };
	FOR $i IN 100..105 { CREATE type::record('w2', $i) SET n = $i - 99 RETURN NONE; };
	FOR $i IN 1..64 { CREATE type::record('w3', $i) SET n = IF $i = 1 { 9dec } ELSE IF $i % 2 = 0 { 9 } ELSE { 9f } RETURN NONE; };
	FOR $i IN 200..201 { CREATE type::record('w3', $i) SET n = 2 RETURN NONE; };
	FOR $i IN 1..65 { CREATE type::record('w4', $i) SET n = IF $i = 1 { 9dec } ELSE IF $i % 2 = 0 { 9 } ELSE { 9f } RETURN NONE; };
	FOR $i IN 200..201 { CREATE type::record('w4', $i) SET n = 2 RETURN NONE; };
	FOR $i IN 1..66 { CREATE type::record('w5', $i) SET n = IF $i = 1 { 9dec } ELSE IF $i % 2 = 0 { 9 } ELSE { 9f } RETURN NONE; };
	FOR $i IN 200..201 { CREATE type::record('w5', $i) SET n = 2 RETURN NONE; };
	FOR $i IN 1..201 { CREATE type::record('w6', $i) SET n = 1000 - $i RETURN NONE; };
	FOR $i IN 1..201 { CREATE type::record('w7', $i) SET n = $i RETURN NONE; };
	FOR $i IN 1..301 { CREATE type::record('w8', $i) SET n = IF $i % 2 = 0 { ($i * 37 % 10) * 1f } ELSE { $i * 37 % 10 } RETURN NONE; };
	FOR $i IN 1..101 { CREATE type::record('w9', $i) SET n = IF $i % 10 = 0 { 'x' } ELSE IF $i % 10 = 5 { NULL } ELSE IF $i % 2 = 0 { 40f } ELSE { 40 } RETURN NONE; };
	FOR $i IN 200..230 { CREATE type::record('w9', $i) SET n = -40 - $i % 2 RETURN NONE; };
	CREATE w10:b SET n = 8 RETURN NONE;
	CREATE w10:a SET n = 8f RETURN NONE;
	CREATE w10:d SET n = 8 RETURN NONE;
	CREATE w10:c SET n = 8dec RETURN NONE;
	CREATE w10:e SET n = 1 RETURN NONE;
	RETURN 'OK';
};

DEFINE FIELD n ON w1 TYPE int;
DEFINE INDEX n ON w1 FIELDS n;
DEFINE FIELD n ON w2 TYPE int;
DEFINE INDEX n ON w2 FIELDS n;
DEFINE FIELD n ON w3 TYPE int;
DEFINE INDEX n ON w3 FIELDS n;
DEFINE FIELD n ON w4 TYPE int;
DEFINE INDEX n ON w4 FIELDS n;
DEFINE FIELD n ON w5 TYPE int;
DEFINE INDEX n ON w5 FIELDS n;
DEFINE FIELD n ON w6 TYPE int;
DEFINE INDEX n ON w6 FIELDS n;
DEFINE FIELD n ON w7 TYPE int;
DEFINE INDEX n ON w7 FIELDS n;
DEFINE FIELD n ON w8 TYPE int;
DEFINE INDEX n ON w8 FIELDS n;
DEFINE FIELD n ON w9 TYPE int;
DEFINE INDEX n ON w9 FIELDS n;
DEFINE FIELD n ON w10 TYPE int;
DEFINE INDEX n ON w10 FIELDS n;

SELECT math::max(n) AS m FROM w1 GROUP ALL;
SELECT math::min(n) AS m FROM w1 GROUP ALL;
SELECT math::max(n) AS m FROM w2 GROUP ALL;
SELECT math::min(n) AS m FROM w2 GROUP ALL;
SELECT math::max(n) AS m FROM w3 GROUP ALL;
SELECT math::min(n) AS m FROM w3 GROUP ALL;
SELECT math::max(n) AS m FROM w4 GROUP ALL;
SELECT math::min(n) AS m FROM w4 GROUP ALL;
SELECT math::max(n) AS m FROM w5 GROUP ALL;
SELECT math::min(n) AS m FROM w5 GROUP ALL;
SELECT math::max(n) AS m FROM w6 GROUP ALL;
SELECT math::min(n) AS m FROM w6 GROUP ALL;
SELECT math::max(n) AS m FROM w7 GROUP ALL;
SELECT math::min(n) AS m FROM w7 GROUP ALL;
SELECT math::max(n) AS m FROM w8 GROUP ALL;
SELECT math::min(n) AS m FROM w8 GROUP ALL;
SELECT math::max(n) AS m FROM w9 GROUP ALL;
SELECT math::min(n) AS m FROM w9 GROUP ALL;
SELECT math::max(n) AS m FROM w10 GROUP ALL;
SELECT math::min(n) AS m FROM w10 GROUP ALL;
RETURN (SELECT VALUE math::max(n) FROM w1 GROUP ALL)[0];
RETURN (SELECT VALUE math::min(n) FROM w1 GROUP ALL)[0];
RETURN (SELECT VALUE math::max(n) FROM w2 GROUP ALL)[0];
RETURN (SELECT VALUE math::min(n) FROM w2 GROUP ALL)[0];
RETURN (SELECT VALUE math::max(n) FROM w3 GROUP ALL)[0];
RETURN (SELECT VALUE math::min(n) FROM w3 GROUP ALL)[0];
RETURN (SELECT VALUE math::max(n) FROM w4 GROUP ALL)[0];
RETURN (SELECT VALUE math::min(n) FROM w4 GROUP ALL)[0];
RETURN (SELECT VALUE math::max(n) FROM w5 GROUP ALL)[0];
RETURN (SELECT VALUE math::min(n) FROM w5 GROUP ALL)[0];
RETURN (SELECT VALUE math::max(n) FROM w6 GROUP ALL)[0];
RETURN (SELECT VALUE math::min(n) FROM w6 GROUP ALL)[0];
RETURN (SELECT VALUE math::max(n) FROM w7 GROUP ALL)[0];
RETURN (SELECT VALUE math::min(n) FROM w7 GROUP ALL)[0];
RETURN (SELECT VALUE math::max(n) FROM w8 GROUP ALL)[0];
RETURN (SELECT VALUE math::min(n) FROM w8 GROUP ALL)[0];
RETURN (SELECT VALUE math::max(n) FROM w9 GROUP ALL)[0];
RETURN (SELECT VALUE math::min(n) FROM w9 GROUP ALL)[0];
RETURN (SELECT VALUE math::max(n) FROM w10 GROUP ALL)[0];
RETURN (SELECT VALUE math::min(n) FROM w10 GROUP ALL)[0];
SELECT math::max(n) AS m FROM w1 GROUP ALL LIMIT 1 START 0;
SELECT VALUE math::max(n) FROM ONLY w3 GROUP ALL;
SELECT id, (SELECT VALUE math::max(n) FROM w2 GROUP ALL)[0] AS hi FROM w10 ORDER BY id;
