/**
[env]
namespace = true
database = true
timeout = 120000

[test]
reason = """
	The extreme of an indexed `int` field whose rows were written before the declaration, over
	equal numbers of different kinds that the index keeps under different keys: a float and a
	decimal of the same digits, in both orders of record id, beside rows that are not equal to
	them. The first of the equal numbers in record id order answers, with its kind, whichever key
	the index lists it under first.
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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ m: 0.1f }]"

[[test.results]]
value = "[{ m: 0.1f }]"

[[test.results]]
value = "[{ m: 0.1dec }]"

[[test.results]]
value = "[{ m: 0.1dec }]"

[[test.results]]
value = "[{ m: 9 }]"

[[test.results]]
value = "[{ m: 9 }]"

[[test.results]]
value = "[{ m: 9dec }]"

[[test.results]]
value = "[{ m: 9dec }]"

[[test.results]]
value = "[{ m: 9f }]"

[[test.results]]
value = "[{ m: 9f }]"

[[test.results]]
value = "[{ m: 9f }]"

[[test.results]]
value = "[{ m: 9f }]"

[[test.results]]
value = "[{ m: 0.3f }]"

[[test.results]]
value = "[{ m: 0.3f }]"

[[test.results]]
value = "[{ m: 0.3dec }]"

[[test.results]]
value = "[{ m: 0.3dec }]"

[[test.results]]
value = "[{ m: 1.5f }]"

[[test.results]]
value = "[{ m: 1.5f }]"

[[test.results]]
value = "[{ m: 1.5dec }]"

[[test.results]]
value = "[{ m: 1.5dec }]"

[[test.results]]
value = "[{ m: -0.1f }]"

[[test.results]]
value = "[{ m: -0.1f }]"

[[test.results]]
value = "[{ m: -0.1dec }]"

[[test.results]]
value = "[{ m: -0.1dec }]"

[[test.results]]
value = "[{ m: 2.5f }]"

[[test.results]]
value = "[{ m: 2.5f }]"

[[test.results]]
value = "[{ m: 2.5dec }]"

[[test.results]]
value = "[{ m: 2.5dec }]"

[[test.results]]
value = "[{ m: 1001 }]"

[[test.results]]
value = "[{ m: 100000000000000000000dec }]"
*/
{
	CREATE p1a:1 SET n = 0.1f RETURN NONE;
	CREATE p1a:2 SET n = 0.1dec RETURN NONE;
	FOR $i IN 1..400 { CREATE type::record('p1a', 100 + $i) SET n = 1000 + $i RETURN NONE; };
	CREATE p1b:1 SET n = 0.1f RETURN NONE;
	CREATE p1b:2 SET n = 0.1dec RETURN NONE;
	FOR $i IN 1..400 { CREATE type::record('p1b', 100 + $i) SET n = -1000 - $i RETURN NONE; };
	CREATE p2a:1 SET n = 0.1dec RETURN NONE;
	CREATE p2a:2 SET n = 0.1f RETURN NONE;
	FOR $i IN 1..400 { CREATE type::record('p2a', 100 + $i) SET n = 1000 + $i RETURN NONE; };
	CREATE p2b:1 SET n = 0.1dec RETURN NONE;
	CREATE p2b:2 SET n = 0.1f RETURN NONE;
	FOR $i IN 1..400 { CREATE type::record('p2b', 100 + $i) SET n = -1000 - $i RETURN NONE; };
	CREATE p3a:1 SET n = 9 RETURN NONE;
	CREATE p3a:2 SET n = 9dec RETURN NONE;
	CREATE p3a:3 SET n = 9f RETURN NONE;
	FOR $i IN 1..400 { CREATE type::record('p3a', 100 + $i) SET n = 1000 + $i RETURN NONE; };
	CREATE p3b:1 SET n = 9 RETURN NONE;
	CREATE p3b:2 SET n = 9dec RETURN NONE;
	CREATE p3b:3 SET n = 9f RETURN NONE;
	FOR $i IN 1..400 { CREATE type::record('p3b', 100 + $i) SET n = -1000 - $i RETURN NONE; };
	CREATE p4a:1 SET n = 9dec RETURN NONE;
	CREATE p4a:2 SET n = 9 RETURN NONE;
	CREATE p4a:3 SET n = 9f RETURN NONE;
	FOR $i IN 1..400 { CREATE type::record('p4a', 100 + $i) SET n = 1000 + $i RETURN NONE; };
	CREATE p4b:1 SET n = 9dec RETURN NONE;
	CREATE p4b:2 SET n = 9 RETURN NONE;
	CREATE p4b:3 SET n = 9f RETURN NONE;
	FOR $i IN 1..400 { CREATE type::record('p4b', 100 + $i) SET n = -1000 - $i RETURN NONE; };
	CREATE p5a:1 SET n = 9f RETURN NONE;
	CREATE p5a:2 SET n = 9dec RETURN NONE;
	CREATE p5a:3 SET n = 9 RETURN NONE;
	FOR $i IN 1..400 { CREATE type::record('p5a', 100 + $i) SET n = 1000 + $i RETURN NONE; };
	CREATE p5b:1 SET n = 9f RETURN NONE;
	CREATE p5b:2 SET n = 9dec RETURN NONE;
	CREATE p5b:3 SET n = 9 RETURN NONE;
	FOR $i IN 1..400 { CREATE type::record('p5b', 100 + $i) SET n = -1000 - $i RETURN NONE; };
	CREATE p6a:1 SET n = 9f RETURN NONE;
	CREATE p6a:2 SET n = 9 RETURN NONE;
	CREATE p6a:3 SET n = 9dec RETURN NONE;
	FOR $i IN 1..400 { CREATE type::record('p6a', 100 + $i) SET n = 1000 + $i RETURN NONE; };
	CREATE p6b:1 SET n = 9f RETURN NONE;
	CREATE p6b:2 SET n = 9 RETURN NONE;
	CREATE p6b:3 SET n = 9dec RETURN NONE;
	FOR $i IN 1..400 { CREATE type::record('p6b', 100 + $i) SET n = -1000 - $i RETURN NONE; };
	CREATE p7a:1 SET n = 0.3f RETURN NONE;
	CREATE p7a:2 SET n = 0.3dec RETURN NONE;
	CREATE p7a:3 SET n = 0.3f RETURN NONE;
	FOR $i IN 1..400 { CREATE type::record('p7a', 100 + $i) SET n = 1000 + $i RETURN NONE; };
	CREATE p7b:1 SET n = 0.3f RETURN NONE;
	CREATE p7b:2 SET n = 0.3dec RETURN NONE;
	CREATE p7b:3 SET n = 0.3f RETURN NONE;
	FOR $i IN 1..400 { CREATE type::record('p7b', 100 + $i) SET n = -1000 - $i RETURN NONE; };
	CREATE p8a:1 SET n = 0.3dec RETURN NONE;
	CREATE p8a:2 SET n = 0.3f RETURN NONE;
	CREATE p8a:3 SET n = 0.3dec RETURN NONE;
	FOR $i IN 1..400 { CREATE type::record('p8a', 100 + $i) SET n = 1000 + $i RETURN NONE; };
	CREATE p8b:1 SET n = 0.3dec RETURN NONE;
	CREATE p8b:2 SET n = 0.3f RETURN NONE;
	CREATE p8b:3 SET n = 0.3dec RETURN NONE;
	FOR $i IN 1..400 { CREATE type::record('p8b', 100 + $i) SET n = -1000 - $i RETURN NONE; };
	CREATE p9a:1 SET n = 1.5f RETURN NONE;
	CREATE p9a:2 SET n = 1.5dec RETURN NONE;
	FOR $i IN 1..400 { CREATE type::record('p9a', 100 + $i) SET n = 1000 + $i RETURN NONE; };
	CREATE p9b:1 SET n = 1.5f RETURN NONE;
	CREATE p9b:2 SET n = 1.5dec RETURN NONE;
	FOR $i IN 1..400 { CREATE type::record('p9b', 100 + $i) SET n = -1000 - $i RETURN NONE; };
	CREATE p10a:1 SET n = 1.5dec RETURN NONE;
	CREATE p10a:2 SET n = 1.5f RETURN NONE;
	FOR $i IN 1..400 { CREATE type::record('p10a', 100 + $i) SET n = 1000 + $i RETURN NONE; };
	CREATE p10b:1 SET n = 1.5dec RETURN NONE;
	CREATE p10b:2 SET n = 1.5f RETURN NONE;
	FOR $i IN 1..400 { CREATE type::record('p10b', 100 + $i) SET n = -1000 - $i RETURN NONE; };
	CREATE p11a:1 SET n = -0.1f RETURN NONE;
	CREATE p11a:2 SET n = -0.1dec RETURN NONE;
	FOR $i IN 1..400 { CREATE type::record('p11a', 100 + $i) SET n = 1000 + $i RETURN NONE; };
	CREATE p11b:1 SET n = -0.1f RETURN NONE;
	CREATE p11b:2 SET n = -0.1dec RETURN NONE;
	FOR $i IN 1..400 { CREATE type::record('p11b', 100 + $i) SET n = -1000 - $i RETURN NONE; };
	CREATE p12a:1 SET n = -0.1dec RETURN NONE;
	CREATE p12a:2 SET n = -0.1f RETURN NONE;
	FOR $i IN 1..400 { CREATE type::record('p12a', 100 + $i) SET n = 1000 + $i RETURN NONE; };
	CREATE p12b:1 SET n = -0.1dec RETURN NONE;
	CREATE p12b:2 SET n = -0.1f RETURN NONE;
	FOR $i IN 1..400 { CREATE type::record('p12b', 100 + $i) SET n = -1000 - $i RETURN NONE; };
	CREATE p13a:1 SET n = 2.5f RETURN NONE;
	CREATE p13a:2 SET n = 2.5 RETURN NONE;
	CREATE p13a:3 SET n = 2.5dec RETURN NONE;
	FOR $i IN 1..400 { CREATE type::record('p13a', 100 + $i) SET n = 1000 + $i RETURN NONE; };
	CREATE p13b:1 SET n = 2.5f RETURN NONE;
	CREATE p13b:2 SET n = 2.5 RETURN NONE;
	CREATE p13b:3 SET n = 2.5dec RETURN NONE;
	FOR $i IN 1..400 { CREATE type::record('p13b', 100 + $i) SET n = -1000 - $i RETURN NONE; };
	CREATE p14a:1 SET n = 2.5dec RETURN NONE;
	CREATE p14a:2 SET n = 2.5 RETURN NONE;
	CREATE p14a:3 SET n = 2.5f RETURN NONE;
	FOR $i IN 1..400 { CREATE type::record('p14a', 100 + $i) SET n = 1000 + $i RETURN NONE; };
	CREATE p14b:1 SET n = 2.5dec RETURN NONE;
	CREATE p14b:2 SET n = 2.5 RETURN NONE;
	CREATE p14b:3 SET n = 2.5f RETURN NONE;
	FOR $i IN 1..400 { CREATE type::record('p14b', 100 + $i) SET n = -1000 - $i RETURN NONE; };
	CREATE p15a:1 SET n = 100000000000000000000dec RETURN NONE;
	CREATE p15a:2 SET n = 1e20f RETURN NONE;
	FOR $i IN 1..400 { CREATE type::record('p15a', 100 + $i) SET n = 1000 + $i RETURN NONE; };
	CREATE p15b:1 SET n = 100000000000000000000dec RETURN NONE;
	CREATE p15b:2 SET n = 1e20f RETURN NONE;
	FOR $i IN 1..400 { CREATE type::record('p15b', 100 + $i) SET n = -1000 - $i RETURN NONE; };
	RETURN 'OK';
};
DEFINE FIELD n ON p1a TYPE int; DEFINE INDEX n ON p1a FIELDS n;
DEFINE FIELD n ON p1b TYPE int; DEFINE INDEX n ON p1b FIELDS n;
DEFINE FIELD n ON p2a TYPE int; DEFINE INDEX n ON p2a FIELDS n;
DEFINE FIELD n ON p2b TYPE int; DEFINE INDEX n ON p2b FIELDS n;
DEFINE FIELD n ON p3a TYPE int; DEFINE INDEX n ON p3a FIELDS n;
DEFINE FIELD n ON p3b TYPE int; DEFINE INDEX n ON p3b FIELDS n;
DEFINE FIELD n ON p4a TYPE int; DEFINE INDEX n ON p4a FIELDS n;
DEFINE FIELD n ON p4b TYPE int; DEFINE INDEX n ON p4b FIELDS n;
DEFINE FIELD n ON p5a TYPE int; DEFINE INDEX n ON p5a FIELDS n;
DEFINE FIELD n ON p5b TYPE int; DEFINE INDEX n ON p5b FIELDS n;
DEFINE FIELD n ON p6a TYPE int; DEFINE INDEX n ON p6a FIELDS n;
DEFINE FIELD n ON p6b TYPE int; DEFINE INDEX n ON p6b FIELDS n;
DEFINE FIELD n ON p7a TYPE int; DEFINE INDEX n ON p7a FIELDS n;
DEFINE FIELD n ON p7b TYPE int; DEFINE INDEX n ON p7b FIELDS n;
DEFINE FIELD n ON p8a TYPE int; DEFINE INDEX n ON p8a FIELDS n;
DEFINE FIELD n ON p8b TYPE int; DEFINE INDEX n ON p8b FIELDS n;
DEFINE FIELD n ON p9a TYPE int; DEFINE INDEX n ON p9a FIELDS n;
DEFINE FIELD n ON p9b TYPE int; DEFINE INDEX n ON p9b FIELDS n;
DEFINE FIELD n ON p10a TYPE int; DEFINE INDEX n ON p10a FIELDS n;
DEFINE FIELD n ON p10b TYPE int; DEFINE INDEX n ON p10b FIELDS n;
DEFINE FIELD n ON p11a TYPE int; DEFINE INDEX n ON p11a FIELDS n;
DEFINE FIELD n ON p11b TYPE int; DEFINE INDEX n ON p11b FIELDS n;
DEFINE FIELD n ON p12a TYPE int; DEFINE INDEX n ON p12a FIELDS n;
DEFINE FIELD n ON p12b TYPE int; DEFINE INDEX n ON p12b FIELDS n;
DEFINE FIELD n ON p13a TYPE int; DEFINE INDEX n ON p13a FIELDS n;
DEFINE FIELD n ON p13b TYPE int; DEFINE INDEX n ON p13b FIELDS n;
DEFINE FIELD n ON p14a TYPE int; DEFINE INDEX n ON p14a FIELDS n;
DEFINE FIELD n ON p14b TYPE int; DEFINE INDEX n ON p14b FIELDS n;
DEFINE FIELD n ON p15a TYPE int; DEFINE INDEX n ON p15a FIELDS n;
DEFINE FIELD n ON p15b TYPE int; DEFINE INDEX n ON p15b FIELDS n;
SELECT math::min(n) AS m FROM p1a GROUP ALL;
SELECT math::max(n) AS m FROM p1b GROUP ALL;
SELECT math::min(n) AS m FROM p2a GROUP ALL;
SELECT math::max(n) AS m FROM p2b GROUP ALL;
SELECT math::min(n) AS m FROM p3a GROUP ALL;
SELECT math::max(n) AS m FROM p3b GROUP ALL;
SELECT math::min(n) AS m FROM p4a GROUP ALL;
SELECT math::max(n) AS m FROM p4b GROUP ALL;
SELECT math::min(n) AS m FROM p5a GROUP ALL;
SELECT math::max(n) AS m FROM p5b GROUP ALL;
SELECT math::min(n) AS m FROM p6a GROUP ALL;
SELECT math::max(n) AS m FROM p6b GROUP ALL;
SELECT math::min(n) AS m FROM p7a GROUP ALL;
SELECT math::max(n) AS m FROM p7b GROUP ALL;
SELECT math::min(n) AS m FROM p8a GROUP ALL;
SELECT math::max(n) AS m FROM p8b GROUP ALL;
SELECT math::min(n) AS m FROM p9a GROUP ALL;
SELECT math::max(n) AS m FROM p9b GROUP ALL;
SELECT math::min(n) AS m FROM p10a GROUP ALL;
SELECT math::max(n) AS m FROM p10b GROUP ALL;
SELECT math::min(n) AS m FROM p11a GROUP ALL;
SELECT math::max(n) AS m FROM p11b GROUP ALL;
SELECT math::min(n) AS m FROM p12a GROUP ALL;
SELECT math::max(n) AS m FROM p12b GROUP ALL;
SELECT math::min(n) AS m FROM p13a GROUP ALL;
SELECT math::max(n) AS m FROM p13b GROUP ALL;
SELECT math::min(n) AS m FROM p14a GROUP ALL;
SELECT math::max(n) AS m FROM p14b GROUP ALL;
SELECT math::min(n) AS m FROM p15a GROUP ALL;
SELECT math::max(n) AS m FROM p15b GROUP ALL;
