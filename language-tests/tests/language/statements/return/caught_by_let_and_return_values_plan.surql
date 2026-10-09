/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
Plan companion to caught_by_let_and_return_values: a LET whose value is a block
holding an IF with a RETURN in it plans as a Let over a Sequence, with the IF
folded to its taken branch, so the RETURN is a step of the nested Sequence.
"""

[[test.results]]
value = '''
'Sequence [ctx: Rt] [statements: 2]
    Let [ctx: Rt] [name: $x]
        Sequence [ctx: Rt] [statements: 2]
            Sequence [ctx: Rt] [statements: 1]
                Return [ctx: Rt]
                    ExprPlan [ctx: Rt] [expr: 1]
            ExprPlan [ctx: Rt] [expr: 2]
    Return [ctx: Rt]
        ExprPlan [ctx: Rt] [expr: $x]
''''
*/

EXPLAIN { LET $x = { IF true { RETURN 1 }; 2 }; RETURN $x };
