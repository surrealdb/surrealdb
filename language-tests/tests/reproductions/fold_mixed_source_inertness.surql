/**
[env]

[test]
reason = """
	A fold that eliminates a subtree must classify every FROM source, not just
	reject any list that is not all tables: a scalar source dereferences nothing
	and keeps the table side collapsing to EmptyScan, while an array source or a
	parameter bound where the statement runs can yield a record id and must hold
	the condition. A parameter bound nowhere is the scalar NONE.
"""

[[test.results]]
value = "[{ a: 5, id: t:1 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Filter [ctx: Db] [predicate: false]
        Union [ctx: Db]
            Union [ctx: Db]
            Values [ctx: Rt] [expr: 1]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Filter [ctx: Db] [predicate: false]
        Union [ctx: Db]
            Union [ctx: Db]
            Values [ctx: Rt] [expr: { b: 1 }]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Filter [ctx: Db] [predicate: false]
        Union [ctx: Db]
            Union [ctx: Db]
            Values [ctx: Rt] [expr: math::PI]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Filter [ctx: Db] [predicate: a INSIDE []]
        Union [ctx: Db]
            TableScan [ctx: Db] [table: t, direction: Forward]
            Values [ctx: Rt] [expr: [1, 2]]
''''

[[test.results]]
value = '''
'Sequence [ctx: Rt] [statements: 2]
    Let [ctx: Rt] [name: $p]
        ExprPlan [ctx: Rt] [expr: [1, 2]]
    SelectProject [ctx: Db] [projections: *]
        Filter [ctx: Db] [predicate: a INSIDE []]
            Union [ctx: Db]
                TableScan [ctx: Db] [table: t, direction: Forward]
                Values [ctx: Rt] [expr: $p]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Filter [ctx: Db] [predicate: false]
        Union [ctx: Db]
            Union [ctx: Db]
            Values [ctx: Rt] [expr: NONE]
''''

*/

CREATE t:1 SET a = 5;

-- A scalar source yields that scalar, and a field read off a scalar is NONE,
-- so the condition still folds and the table side selects EmptyScan.
EXPLAIN SELECT * FROM t, 1 WHERE a IN [];

-- An object row's bare field read is a key lookup, same as a record's.
EXPLAIN SELECT * FROM t, { b: 1 } WHERE a IN [];

-- A constant yields a float, datetime or duration, so it is a scalar row too.
EXPLAIN SELECT * FROM t, math::PI WHERE a IN [];

-- An array source can yield a record id, where the same read dereferences
-- it and runs computed fields and permissions, so the condition has to
-- survive planning and the table is scanned.
EXPLAIN SELECT * FROM t, [1, 2] WHERE a IN [];

-- A parameter bound where the statement runs is a value the plan cannot
-- know: the source is resolved per statement and the condition survives.
EXPLAIN { LET $p = [1, 2]; SELECT * FROM t, $p WHERE a IN [] };

-- A parameter bound nowhere is NONE, a scalar source, so the condition folds
-- as it does for a literal NONE.
EXPLAIN SELECT * FROM t, $unbound WHERE a IN [];
