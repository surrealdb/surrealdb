/**
[env]
namespace = true
database = true
auth = { level = "owner" }
planner-strategy = ["all-ro"]

[test]
reason = """
	The plan behind inline_unmentioned_argument: a call whose
	unmentioned argument cannot raise is replaced by its body, one whose
	argument may raise stays a call.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'Return [ctx: Rt]
    Expr [ctx: Rt] [expr: 1]
''''

[[test.results]]
value = '''
'Return [ctx: Db]
    Expr [ctx: Db] [expr: fn::one(<int>  NONE)]
''''
*/

DEFINE FUNCTION fn::one($x: any) { RETURN 1; };
EXPLAIN RETURN fn::one(1);
EXPLAIN RETURN fn::one(<int> NONE);
