/**
[env]
namespace = true
database = true
auth = { level = "owner" }
# Defining 47 views one over another is slow in an unoptimised build.
timeout = 10000

[test]
reason = """
Each row a view's maintenance writes is a write of its own within the write
that fed it, and maintains the views over its table in turn, so a chain of views
nests one write within another for every view in it. Each level spends of the
computation-depth budget what a nested write statement spends, so a chain the
budget covers is maintained to its end, and a deeper one fails the write that
feeds it, which is undone, rather than exhausting the stack. With the default
budget a chain of 23 views is maintained and one of 24 is not.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: a23:1, n: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"
*/

-- 23 views, each over the one before.
DEFINE TABLE a0 SCHEMALESS;
{
	DEFINE TABLE a1 AS SELECT n FROM a0;
	DEFINE TABLE a2 AS SELECT n FROM a1;
	DEFINE TABLE a3 AS SELECT n FROM a2;
	DEFINE TABLE a4 AS SELECT n FROM a3;
	DEFINE TABLE a5 AS SELECT n FROM a4;
	DEFINE TABLE a6 AS SELECT n FROM a5;
	DEFINE TABLE a7 AS SELECT n FROM a6;
	DEFINE TABLE a8 AS SELECT n FROM a7;
	DEFINE TABLE a9 AS SELECT n FROM a8;
	DEFINE TABLE a10 AS SELECT n FROM a9;
	DEFINE TABLE a11 AS SELECT n FROM a10;
	DEFINE TABLE a12 AS SELECT n FROM a11;
	DEFINE TABLE a13 AS SELECT n FROM a12;
	DEFINE TABLE a14 AS SELECT n FROM a13;
	DEFINE TABLE a15 AS SELECT n FROM a14;
	DEFINE TABLE a16 AS SELECT n FROM a15;
	DEFINE TABLE a17 AS SELECT n FROM a16;
	DEFINE TABLE a18 AS SELECT n FROM a17;
	DEFINE TABLE a19 AS SELECT n FROM a18;
	DEFINE TABLE a20 AS SELECT n FROM a19;
	DEFINE TABLE a21 AS SELECT n FROM a20;
	DEFINE TABLE a22 AS SELECT n FROM a21;
	DEFINE TABLE a23 AS SELECT n FROM a22;
};
CREATE a0:1 SET n = 1 RETURN NONE;
SELECT * FROM a23;

-- 24 views: the last is one level past the budget.
DEFINE TABLE b0 SCHEMALESS;
{
	DEFINE TABLE b1 AS SELECT n FROM b0;
	DEFINE TABLE b2 AS SELECT n FROM b1;
	DEFINE TABLE b3 AS SELECT n FROM b2;
	DEFINE TABLE b4 AS SELECT n FROM b3;
	DEFINE TABLE b5 AS SELECT n FROM b4;
	DEFINE TABLE b6 AS SELECT n FROM b5;
	DEFINE TABLE b7 AS SELECT n FROM b6;
	DEFINE TABLE b8 AS SELECT n FROM b7;
	DEFINE TABLE b9 AS SELECT n FROM b8;
	DEFINE TABLE b10 AS SELECT n FROM b9;
	DEFINE TABLE b11 AS SELECT n FROM b10;
	DEFINE TABLE b12 AS SELECT n FROM b11;
	DEFINE TABLE b13 AS SELECT n FROM b12;
	DEFINE TABLE b14 AS SELECT n FROM b13;
	DEFINE TABLE b15 AS SELECT n FROM b14;
	DEFINE TABLE b16 AS SELECT n FROM b15;
	DEFINE TABLE b17 AS SELECT n FROM b16;
	DEFINE TABLE b18 AS SELECT n FROM b17;
	DEFINE TABLE b19 AS SELECT n FROM b18;
	DEFINE TABLE b20 AS SELECT n FROM b19;
	DEFINE TABLE b21 AS SELECT n FROM b20;
	DEFINE TABLE b22 AS SELECT n FROM b21;
	DEFINE TABLE b23 AS SELECT n FROM b22;
	DEFINE TABLE b24 AS SELECT n FROM b23;
};
CREATE b0:1 SET n = 1 RETURN NONE;
SELECT * FROM b0;
SELECT * FROM b23;

