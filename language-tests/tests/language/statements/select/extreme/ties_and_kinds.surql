/**
[env]
namespace = true
database = true
timeout = 20000

[test]
reason = """
	The extreme of an indexed `int` field whose rows were written before the declaration. The
	index keeps one key for equal numbers of every kind, so the answer's kind and the row it comes
	from are the first of the equal numbers in record id order, as the fold over the rows keeps
	them. Covers zero beside negative zero, a float beside a decimal of the same digits (two numbers,
	so the extreme is the one their exact values put first), integers
	past the range a float holds exactly, NaN, an infinity, three kinds of one number in every
	order, equal numbers of one kind, rows that are not numbers in every position, and one row.
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
value = "[{ m: 0 }]"

[[test.results]]
value = "[{ m: -5 }]"

[[test.results]]
value = "[{ m: -0f }]"

[[test.results]]
value = "[{ m: -5 }]"

[[test.results]]
value = "[{ m: 5 }]"

[[test.results]]
value = "[{ m: 0 }]"

[[test.results]]
value = "[{ m: 5 }]"

[[test.results]]
value = "[{ m: -0f }]"

[[test.results]]
value = "[{ m: 0.1f }]"

[[test.results]]
value = "[{ m: 0 }]"

[[test.results]]
value = "[{ m: 0.1f }]"

[[test.results]]
value = "[{ m: 0 }]"

[[test.results]]
value = "[{ m: 3 }]"

[[test.results]]
value = "[{ m: 0.1dec }]"

[[test.results]]
value = "[{ m: 3 }]"

[[test.results]]
value = "[{ m: 0.1dec }]"

[[test.results]]
value = "[{ m: 9007199254740993 }]"

[[test.results]]
value = "[{ m: 9007199254740992f }]"

[[test.results]]
value = "[{ m: 9007199254740993 }]"

[[test.results]]
value = "[{ m: 9007199254740992f }]"

[[test.results]]
value = "[{ m: -9007199254740992f }]"

[[test.results]]
value = "[{ m: -9007199254740993 }]"

[[test.results]]
value = "[{ m: -9007199254740992f }]"

[[test.results]]
value = "[{ m: -9007199254740993 }]"

[[test.results]]
value = "[{ m: NaN }]"

[[test.results]]
value = "[{ m: 5 }]"

[[test.results]]
value = "[{ m: NaN }]"

[[test.results]]
value = "[{ m: NaN }]"

[[test.results]]
value = "[{ m: NaN }]"

[[test.results]]
value = "[{ m: Infinity }]"

[[test.results]]
value = "[{ m: NaN }]"

[[test.results]]
value = "[{ m: 5 }]"

[[test.results]]
value = "[{ m: NaN }]"

[[test.results]]
value = "[{ m: NaN }]"

[[test.results]]
value = "[{ m: Infinity }]"

[[test.results]]
value = "[{ m: -Infinity }]"

[[test.results]]
value = "[{ m: 5 }]"

[[test.results]]
value = "[{ m: 1 }]"

[[test.results]]
value = "[{ m: 5dec }]"

[[test.results]]
value = "[{ m: 1 }]"

[[test.results]]
value = "[{ m: 5f }]"

[[test.results]]
value = "[{ m: 1 }]"

[[test.results]]
value = "[{ m: 9 }]"

[[test.results]]
value = "[{ m: 5f }]"

[[test.results]]
value = "[{ m: 9 }]"

[[test.results]]
value = "[{ m: 5dec }]"

[[test.results]]
value = "[{ m: 40 }]"

[[test.results]]
value = "[{ m: 3 }]"

[[test.results]]
value = "[{ m: 3 }]"

[[test.results]]
value = "[{ m: -40 }]"

[[test.results]]
value = "[{ m: 40f }]"

[[test.results]]
value = "[{ m: 3 }]"

[[test.results]]
value = "[{ m: 5 }]"

[[test.results]]
value = "[{ m: 5 }]"

[[test.results]]
value = "[{ m: 5 }]"

[[test.results]]
value = "[{ m: 5 }]"

[[test.results]]
value = "[{ m: 5 }]"

[[test.results]]
value = "[{ m: 5 }]"

[[test.results]]
value = "[{ m: 5 }]"

[[test.results]]
value = "[{ m: 5 }]"

[[test.results]]
value = "[{ m: 5 }]"

[[test.results]]
value = "[{ m: 5 }]"

[[test.results]]
value = "[{ m: 5f }]"

[[test.results]]
value = "[{ m: 5f }]"

[[test.results]]
value = "[{ m: 7 }]"

[[test.results]]
value = "[{ m: 7 }]"

[[test.results]]
value = "[{ m: 7f }]"

[[test.results]]
value = "[{ m: 7f }]"

[[test.results]]
value = "[{ m: 7dec }]"

[[test.results]]
value = "[{ m: 7dec }]"

[[test.results]]
value = "0"

[[test.results]]
value = "-5"

[[test.results]]
value = "-0f"

[[test.results]]
value = "-5"

[[test.results]]
value = "0.1f"

[[test.results]]
value = "0"

[[test.results]]
value = "0.1f"

[[test.results]]
value = "0"

[[test.results]]
value = "9007199254740993"

[[test.results]]
value = "9007199254740992f"

[[test.results]]
value = "9007199254740993"

[[test.results]]
value = "9007199254740992f"

[[test.results]]
value = "NaN"

[[test.results]]
value = "5"

[[test.results]]
value = "5"

[[test.results]]
value = "1"

[[test.results]]
value = "5dec"

[[test.results]]
value = "1"

[[test.results]]
value = "5f"

[[test.results]]
value = "1"

[[test.results]]
value = "9"

[[test.results]]
value = "5f"

[[test.results]]
value = "9"

[[test.results]]
value = "5dec"

[[test.results]]
value = "40"

[[test.results]]
value = "3"

[[test.results]]
value = "40f"

[[test.results]]
value = "3"

[[test.results]]
value = "5"

[[test.results]]
value = "5"

[[test.results]]
value = "5f"

[[test.results]]
value = "5f"

[[test.results]]
value = "7f"

[[test.results]]
value = "7f"

[[test.results]]
value = "7dec"

[[test.results]]
value = "7dec"

[[test.results]]
value = "[{ m: 5 }]"

[[test.results]]
value = "[{ m: 1 }]"

[[test.results]]
value = "5"

[[test.results]]
value = "[{ m: 5dec }]"

[[test.results]]
value = "[{ m: 1 }]"

[[test.results]]
value = "5dec"

[[test.results]]
value = "[{ m: 0 }]"

[[test.results]]
value = "[{ m: -5 }]"

[[test.results]]
value = "0"

[[test.results]]
value = "[{ m: 5 }]"

[[test.results]]
value = "[{ m: 0 }]"

[[test.results]]
value = "5"

[[test.results]]
value = "[{ hi: 5, id: s1:1, lo: 1 }]"

[[test.results]]
value = "[{ id: t1:1 }, { id: t1:2 }, { id: t1:3 }]"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

*/

{
	CREATE z1:1 SET n = 0 RETURN NONE;
	CREATE z1:2 SET n = -0f RETURN NONE;
	CREATE z1:3 SET n = -5 RETURN NONE;
	CREATE z2:1 SET n = -0f RETURN NONE;
	CREATE z2:2 SET n = 0 RETURN NONE;
	CREATE z2:3 SET n = -5 RETURN NONE;
	CREATE z3:1 SET n = 5 RETURN NONE;
	CREATE z3:2 SET n = 0 RETURN NONE;
	CREATE z3:3 SET n = -0f RETURN NONE;
	CREATE z4:1 SET n = 5 RETURN NONE;
	CREATE z4:2 SET n = -0f RETURN NONE;
	CREATE z4:3 SET n = 0 RETURN NONE;
	CREATE d1:1 SET n = 0.1f RETURN NONE;
	CREATE d1:2 SET n = 0.1dec RETURN NONE;
	CREATE d1:3 SET n = 0 RETURN NONE;
	CREATE d2:1 SET n = 0.1dec RETURN NONE;
	CREATE d2:2 SET n = 0.1f RETURN NONE;
	CREATE d2:3 SET n = 0 RETURN NONE;
	CREATE d3:1 SET n = 3 RETURN NONE;
	CREATE d3:2 SET n = 0.1f RETURN NONE;
	CREATE d3:3 SET n = 0.1dec RETURN NONE;
	CREATE d4:1 SET n = 3 RETURN NONE;
	CREATE d4:2 SET n = 0.1dec RETURN NONE;
	CREATE d4:3 SET n = 0.1f RETURN NONE;
	CREATE g1:1 SET n = 9007199254740993 RETURN NONE;
	CREATE g1:2 SET n = 9007199254740992.0f RETURN NONE;
	CREATE g2:1 SET n = 9007199254740992.0f RETURN NONE;
	CREATE g2:2 SET n = 9007199254740993 RETURN NONE;
	CREATE g3:1 SET n = -9007199254740993 RETURN NONE;
	CREATE g3:2 SET n = -9007199254740992.0f RETURN NONE;
	CREATE g4:1 SET n = -9007199254740992.0f RETURN NONE;
	CREATE g4:2 SET n = -9007199254740993 RETURN NONE;
	CREATE n1:1 SET n = NaN RETURN NONE;
	CREATE n1:2 SET n = NaN RETURN NONE;
	CREATE n1:3 SET n = 5 RETURN NONE;
	CREATE n2:1 SET n = NaN RETURN NONE;
	CREATE n2:2 SET n = NaN RETURN NONE;
	CREATE n3:1 SET n = NaN RETURN NONE;
	CREATE n3:2 SET n = math::inf RETURN NONE;
	CREATE n4:1 SET n = 5 RETURN NONE;
	CREATE n4:2 SET n = NaN RETURN NONE;
	CREATE n4:3 SET n = 7 RETURN NONE;
	CREATE n5:1 SET n = NaN RETURN NONE;
	CREATE n6:1 SET n = math::inf RETURN NONE;
	CREATE n6:2 SET n = -math::inf RETURN NONE;
	CREATE t1:1 SET n = 5 RETURN NONE;
	CREATE t1:2 SET n = 5f RETURN NONE;
	CREATE t1:3 SET n = 5dec RETURN NONE;
	CREATE t1:4 SET n = 1 RETURN NONE;
	CREATE t2:1 SET n = 5dec RETURN NONE;
	CREATE t2:2 SET n = 5f RETURN NONE;
	CREATE t2:3 SET n = 5 RETURN NONE;
	CREATE t2:4 SET n = 1 RETURN NONE;
	CREATE t3:1 SET n = 1 RETURN NONE;
	CREATE t3:2 SET n = 5f RETURN NONE;
	CREATE t3:3 SET n = 5 RETURN NONE;
	CREATE t3:4 SET n = 5dec RETURN NONE;
	CREATE t4:1 SET n = 5f RETURN NONE;
	CREATE t4:2 SET n = 5 RETURN NONE;
	CREATE t4:3 SET n = 5dec RETURN NONE;
	CREATE t4:4 SET n = 9 RETURN NONE;
	CREATE t5:1 SET n = 9 RETURN NONE;
	CREATE t5:2 SET n = 5dec RETURN NONE;
	CREATE t5:3 SET n = 5f RETURN NONE;
	CREATE t5:4 SET n = 5 RETURN NONE;
	CREATE e1:1 SET n = 40 RETURN NONE;
	CREATE e1:2 SET n = 40 RETURN NONE;
	CREATE e1:3 SET n = 3 RETURN NONE;
	CREATE e2:1 SET n = -40 RETURN NONE;
	CREATE e2:2 SET n = -40 RETURN NONE;
	CREATE e2:3 SET n = 3 RETURN NONE;
	CREATE e3:1 SET n = 40f RETURN NONE;
	CREATE e3:2 SET n = 40f RETURN NONE;
	CREATE e3:3 SET n = 3 RETURN NONE;
	CREATE m1:1 SET n = 'x' RETURN NONE;
	CREATE m1:2 SET n = 5 RETURN NONE;
	CREATE m1:3 SET n = NULL RETURN NONE;
	CREATE m2:1 SET n = NULL RETURN NONE;
	CREATE m2:2 SET n = 5 RETURN NONE;
	CREATE m2:3 SET n = 'x' RETURN NONE;
	CREATE m3:1 SET n = true RETURN NONE;
	CREATE m3:2 SET n = 5 RETURN NONE;
	CREATE m3:3 SET n = [1, 2] RETURN NONE;
	CREATE m4:1 SET n = { a: 1 } RETURN NONE;
	CREATE m4:2 SET n = 5 RETURN NONE;
	CREATE m4:3 SET n = 'x' RETURN NONE;
	CREATE m4:4 SET n = NULL RETURN NONE;
	CREATE m5:1 SET n = 5 RETURN NONE;
	CREATE m5:2 SET n = 'x' RETURN NONE;
	CREATE m5:3 SET n = 5f RETURN NONE;
	CREATE m6:1 SET n = 'x' RETURN NONE;
	CREATE m6:2 SET n = 5f RETURN NONE;
	CREATE m6:3 SET n = 5 RETURN NONE;
	CREATE s1:1 SET n = 7 RETURN NONE;
	CREATE s2:1 SET n = 7f RETURN NONE;
	CREATE s3:1 SET n = 7dec RETURN NONE;
	RETURN 'OK';
};

DEFINE FIELD n ON z1 TYPE int;
DEFINE INDEX n ON z1 FIELDS n;
DEFINE FIELD n ON z2 TYPE int;
DEFINE INDEX n ON z2 FIELDS n;
DEFINE FIELD n ON z3 TYPE int;
DEFINE INDEX n ON z3 FIELDS n;
DEFINE FIELD n ON z4 TYPE int;
DEFINE INDEX n ON z4 FIELDS n;
DEFINE FIELD n ON d1 TYPE int;
DEFINE INDEX n ON d1 FIELDS n;
DEFINE FIELD n ON d2 TYPE int;
DEFINE INDEX n ON d2 FIELDS n;
DEFINE FIELD n ON d3 TYPE int;
DEFINE INDEX n ON d3 FIELDS n;
DEFINE FIELD n ON d4 TYPE int;
DEFINE INDEX n ON d4 FIELDS n;
DEFINE FIELD n ON g1 TYPE int;
DEFINE INDEX n ON g1 FIELDS n;
DEFINE FIELD n ON g2 TYPE int;
DEFINE INDEX n ON g2 FIELDS n;
DEFINE FIELD n ON g3 TYPE int;
DEFINE INDEX n ON g3 FIELDS n;
DEFINE FIELD n ON g4 TYPE int;
DEFINE INDEX n ON g4 FIELDS n;
DEFINE FIELD n ON n1 TYPE int;
DEFINE INDEX n ON n1 FIELDS n;
DEFINE FIELD n ON n2 TYPE int;
DEFINE INDEX n ON n2 FIELDS n;
DEFINE FIELD n ON n3 TYPE int;
DEFINE INDEX n ON n3 FIELDS n;
DEFINE FIELD n ON n4 TYPE int;
DEFINE INDEX n ON n4 FIELDS n;
DEFINE FIELD n ON n5 TYPE int;
DEFINE INDEX n ON n5 FIELDS n;
DEFINE FIELD n ON n6 TYPE int;
DEFINE INDEX n ON n6 FIELDS n;
DEFINE FIELD n ON t1 TYPE int;
DEFINE INDEX n ON t1 FIELDS n;
DEFINE FIELD n ON t2 TYPE int;
DEFINE INDEX n ON t2 FIELDS n;
DEFINE FIELD n ON t3 TYPE int;
DEFINE INDEX n ON t3 FIELDS n;
DEFINE FIELD n ON t4 TYPE int;
DEFINE INDEX n ON t4 FIELDS n;
DEFINE FIELD n ON t5 TYPE int;
DEFINE INDEX n ON t5 FIELDS n;
DEFINE FIELD n ON e1 TYPE int;
DEFINE INDEX n ON e1 FIELDS n;
DEFINE FIELD n ON e2 TYPE int;
DEFINE INDEX n ON e2 FIELDS n;
DEFINE FIELD n ON e3 TYPE int;
DEFINE INDEX n ON e3 FIELDS n;
DEFINE FIELD n ON m1 TYPE int;
DEFINE INDEX n ON m1 FIELDS n;
DEFINE FIELD n ON m2 TYPE int;
DEFINE INDEX n ON m2 FIELDS n;
DEFINE FIELD n ON m3 TYPE int;
DEFINE INDEX n ON m3 FIELDS n;
DEFINE FIELD n ON m4 TYPE int;
DEFINE INDEX n ON m4 FIELDS n;
DEFINE FIELD n ON m5 TYPE int;
DEFINE INDEX n ON m5 FIELDS n;
DEFINE FIELD n ON m6 TYPE int;
DEFINE INDEX n ON m6 FIELDS n;
DEFINE FIELD n ON s1 TYPE int;
DEFINE INDEX n ON s1 FIELDS n;
DEFINE FIELD n ON s2 TYPE int;
DEFINE INDEX n ON s2 FIELDS n;
DEFINE FIELD n ON s3 TYPE int;
DEFINE INDEX n ON s3 FIELDS n;

SELECT math::max(n) AS m FROM z1 GROUP ALL;
SELECT math::min(n) AS m FROM z1 GROUP ALL;
SELECT math::max(n) AS m FROM z2 GROUP ALL;
SELECT math::min(n) AS m FROM z2 GROUP ALL;
SELECT math::max(n) AS m FROM z3 GROUP ALL;
SELECT math::min(n) AS m FROM z3 GROUP ALL;
SELECT math::max(n) AS m FROM z4 GROUP ALL;
SELECT math::min(n) AS m FROM z4 GROUP ALL;
SELECT math::max(n) AS m FROM d1 GROUP ALL;
SELECT math::min(n) AS m FROM d1 GROUP ALL;
SELECT math::max(n) AS m FROM d2 GROUP ALL;
SELECT math::min(n) AS m FROM d2 GROUP ALL;
SELECT math::max(n) AS m FROM d3 GROUP ALL;
SELECT math::min(n) AS m FROM d3 GROUP ALL;
SELECT math::max(n) AS m FROM d4 GROUP ALL;
SELECT math::min(n) AS m FROM d4 GROUP ALL;
SELECT math::max(n) AS m FROM g1 GROUP ALL;
SELECT math::min(n) AS m FROM g1 GROUP ALL;
SELECT math::max(n) AS m FROM g2 GROUP ALL;
SELECT math::min(n) AS m FROM g2 GROUP ALL;
SELECT math::max(n) AS m FROM g3 GROUP ALL;
SELECT math::min(n) AS m FROM g3 GROUP ALL;
SELECT math::max(n) AS m FROM g4 GROUP ALL;
SELECT math::min(n) AS m FROM g4 GROUP ALL;
SELECT math::max(n) AS m FROM n1 GROUP ALL;
SELECT math::min(n) AS m FROM n1 GROUP ALL;
SELECT math::max(n) AS m FROM n2 GROUP ALL;
SELECT math::min(n) AS m FROM n2 GROUP ALL;
SELECT math::max(n) AS m FROM n3 GROUP ALL;
SELECT math::min(n) AS m FROM n3 GROUP ALL;
SELECT math::max(n) AS m FROM n4 GROUP ALL;
SELECT math::min(n) AS m FROM n4 GROUP ALL;
SELECT math::max(n) AS m FROM n5 GROUP ALL;
SELECT math::min(n) AS m FROM n5 GROUP ALL;
SELECT math::max(n) AS m FROM n6 GROUP ALL;
SELECT math::min(n) AS m FROM n6 GROUP ALL;
SELECT math::max(n) AS m FROM t1 GROUP ALL;
SELECT math::min(n) AS m FROM t1 GROUP ALL;
SELECT math::max(n) AS m FROM t2 GROUP ALL;
SELECT math::min(n) AS m FROM t2 GROUP ALL;
SELECT math::max(n) AS m FROM t3 GROUP ALL;
SELECT math::min(n) AS m FROM t3 GROUP ALL;
SELECT math::max(n) AS m FROM t4 GROUP ALL;
SELECT math::min(n) AS m FROM t4 GROUP ALL;
SELECT math::max(n) AS m FROM t5 GROUP ALL;
SELECT math::min(n) AS m FROM t5 GROUP ALL;
SELECT math::max(n) AS m FROM e1 GROUP ALL;
SELECT math::min(n) AS m FROM e1 GROUP ALL;
SELECT math::max(n) AS m FROM e2 GROUP ALL;
SELECT math::min(n) AS m FROM e2 GROUP ALL;
SELECT math::max(n) AS m FROM e3 GROUP ALL;
SELECT math::min(n) AS m FROM e3 GROUP ALL;
SELECT math::max(n) AS m FROM m1 GROUP ALL;
SELECT math::min(n) AS m FROM m1 GROUP ALL;
SELECT math::max(n) AS m FROM m2 GROUP ALL;
SELECT math::min(n) AS m FROM m2 GROUP ALL;
SELECT math::max(n) AS m FROM m3 GROUP ALL;
SELECT math::min(n) AS m FROM m3 GROUP ALL;
SELECT math::max(n) AS m FROM m4 GROUP ALL;
SELECT math::min(n) AS m FROM m4 GROUP ALL;
SELECT math::max(n) AS m FROM m5 GROUP ALL;
SELECT math::min(n) AS m FROM m5 GROUP ALL;
SELECT math::max(n) AS m FROM m6 GROUP ALL;
SELECT math::min(n) AS m FROM m6 GROUP ALL;
SELECT math::max(n) AS m FROM s1 GROUP ALL;
SELECT math::min(n) AS m FROM s1 GROUP ALL;
SELECT math::max(n) AS m FROM s2 GROUP ALL;
SELECT math::min(n) AS m FROM s2 GROUP ALL;
SELECT math::max(n) AS m FROM s3 GROUP ALL;
SELECT math::min(n) AS m FROM s3 GROUP ALL;

-- Value position, paging, and the kind of the answer.
RETURN (SELECT VALUE math::max(n) FROM z1 GROUP ALL)[0];
RETURN (SELECT VALUE math::min(n) FROM z1 GROUP ALL)[0];
RETURN (SELECT VALUE math::max(n) FROM z2 GROUP ALL)[0];
RETURN (SELECT VALUE math::min(n) FROM z2 GROUP ALL)[0];
RETURN (SELECT VALUE math::max(n) FROM d1 GROUP ALL)[0];
RETURN (SELECT VALUE math::min(n) FROM d1 GROUP ALL)[0];
RETURN (SELECT VALUE math::max(n) FROM d2 GROUP ALL)[0];
RETURN (SELECT VALUE math::min(n) FROM d2 GROUP ALL)[0];
RETURN (SELECT VALUE math::max(n) FROM g1 GROUP ALL)[0];
RETURN (SELECT VALUE math::min(n) FROM g1 GROUP ALL)[0];
RETURN (SELECT VALUE math::max(n) FROM g2 GROUP ALL)[0];
RETURN (SELECT VALUE math::min(n) FROM g2 GROUP ALL)[0];
RETURN (SELECT VALUE math::max(n) FROM n1 GROUP ALL)[0];
RETURN (SELECT VALUE math::min(n) FROM n1 GROUP ALL)[0];
RETURN (SELECT VALUE math::max(n) FROM t1 GROUP ALL)[0];
RETURN (SELECT VALUE math::min(n) FROM t1 GROUP ALL)[0];
RETURN (SELECT VALUE math::max(n) FROM t2 GROUP ALL)[0];
RETURN (SELECT VALUE math::min(n) FROM t2 GROUP ALL)[0];
RETURN (SELECT VALUE math::max(n) FROM t3 GROUP ALL)[0];
RETURN (SELECT VALUE math::min(n) FROM t3 GROUP ALL)[0];
RETURN (SELECT VALUE math::max(n) FROM t4 GROUP ALL)[0];
RETURN (SELECT VALUE math::min(n) FROM t4 GROUP ALL)[0];
RETURN (SELECT VALUE math::max(n) FROM t5 GROUP ALL)[0];
RETURN (SELECT VALUE math::min(n) FROM t5 GROUP ALL)[0];
RETURN (SELECT VALUE math::max(n) FROM e1 GROUP ALL)[0];
RETURN (SELECT VALUE math::min(n) FROM e1 GROUP ALL)[0];
RETURN (SELECT VALUE math::max(n) FROM e3 GROUP ALL)[0];
RETURN (SELECT VALUE math::min(n) FROM e3 GROUP ALL)[0];
RETURN (SELECT VALUE math::max(n) FROM m5 GROUP ALL)[0];
RETURN (SELECT VALUE math::min(n) FROM m5 GROUP ALL)[0];
RETURN (SELECT VALUE math::max(n) FROM m6 GROUP ALL)[0];
RETURN (SELECT VALUE math::min(n) FROM m6 GROUP ALL)[0];
RETURN (SELECT VALUE math::max(n) FROM s2 GROUP ALL)[0];
RETURN (SELECT VALUE math::min(n) FROM s2 GROUP ALL)[0];
RETURN (SELECT VALUE math::max(n) FROM s3 GROUP ALL)[0];
RETURN (SELECT VALUE math::min(n) FROM s3 GROUP ALL)[0];
SELECT math::max(n) AS m FROM t1 GROUP ALL LIMIT 1 START 0;
SELECT math::min(n) AS m FROM t1 GROUP ALL LIMIT 1;
SELECT VALUE math::max(n) FROM ONLY t1 GROUP ALL;
SELECT math::max(n) AS m FROM t2 GROUP ALL LIMIT 1 START 0;
SELECT math::min(n) AS m FROM t2 GROUP ALL LIMIT 1;
SELECT VALUE math::max(n) FROM ONLY t2 GROUP ALL;
SELECT math::max(n) AS m FROM z1 GROUP ALL LIMIT 1 START 0;
SELECT math::min(n) AS m FROM z1 GROUP ALL LIMIT 1;
SELECT VALUE math::max(n) FROM ONLY z1 GROUP ALL;
SELECT math::max(n) AS m FROM z3 GROUP ALL LIMIT 1 START 0;
SELECT math::min(n) AS m FROM z3 GROUP ALL LIMIT 1;
SELECT VALUE math::max(n) FROM ONLY z3 GROUP ALL;
SELECT id, (SELECT VALUE math::max(n) FROM t1 GROUP ALL)[0] AS hi, (SELECT VALUE math::min(n) FROM t3 GROUP ALL)[0] AS lo FROM s1 ORDER BY id;
SELECT id FROM t1 WHERE n = (SELECT VALUE math::max(n) FROM t1 GROUP ALL)[0] ORDER BY id;
RETURN type::is_float((SELECT VALUE math::max(n) FROM t2 GROUP ALL)[0]);
RETURN type::is_decimal((SELECT VALUE math::max(n) FROM t2 GROUP ALL)[0]);
RETURN type::is_int((SELECT VALUE math::max(n) FROM t1 GROUP ALL)[0]);
RETURN type::is_decimal((SELECT VALUE math::min(n) FROM t5 GROUP ALL)[0]);
