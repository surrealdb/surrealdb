/**
[env]
namespace = true
database = true

[test]
reason = """
	Plan companion to nested_block_is_a_value_step: a RETURN of a block
	plans as a Return above the block, whose steps appear in order; a block
	nested inside another is one value step of the enclosing block and its
	LET bindings are scoped to it, so the inner block sits as a single child
	of the outer one. A RETURN of a user-defined function call plans as a
	Return above the call expression, with the body left unexpanded.
"""

[[test.results]]
value = '''
'Return [ctx: Rt]
    Sequence [ctx: Rt] [statements: 2]
        Sequence [ctx: Rt] [statements: 1]
            Let [ctx: Rt] [name: $x]
                ExprPlan [ctx: Rt] [expr: 1]
        Return [ctx: Rt]
            ExprPlan [ctx: Rt] [expr: $x]
''''

[[test.results]]
value = '''
'Return [ctx: Rt]
    Sequence [ctx: Rt] [statements: 1]
        Sequence [ctx: Rt] [statements: 2]
            Let [ctx: Rt] [name: $x]
                ExprPlan [ctx: Rt] [expr: 1]
            ExprPlan [ctx: Rt] [expr: 1 + 1]
''''

[[test.results]]
value = '''
'Return [ctx: Rt]
    Sequence [ctx: Rt] [statements: 2]
        Sequence [ctx: Rt] [statements: 2]
            Let [ctx: Rt] [name: $x]
                ExprPlan [ctx: Rt] [expr: 1]
            ExprPlan [ctx: Rt] [expr: 1 + 1]
        ExprPlan [ctx: Rt] [expr: $x]
''''

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'Return [ctx: Rt]
    ExprPlan [ctx: Rt] [expr: fn::leak()]
''''

[[test.results]]
value = '''
'Return [ctx: Rt]
    Sequence [ctx: Rt] [statements: 3]
        Let [ctx: Rt] [name: $y]
            ExprPlan [ctx: Rt] [expr: 5]
        Sequence [ctx: Rt] [statements: 1]
            Let [ctx: Rt] [name: $y]
                ExprPlan [ctx: Rt] [expr: 6]
        ExprPlan [ctx: Rt] [expr: 5]
''''
*/

EXPLAIN RETURN { { LET $x = 1 }; RETURN $x };
EXPLAIN RETURN { { LET $x = 1; $x + 1 } };
EXPLAIN RETURN { { LET $x = 1; $x + 1 }; $x };
DEFINE FUNCTION fn::leak() { { LET $x = 1 }; RETURN $x; };
EXPLAIN RETURN fn::leak();
EXPLAIN RETURN { LET $y = 5; { LET $y = 6 }; $y };
