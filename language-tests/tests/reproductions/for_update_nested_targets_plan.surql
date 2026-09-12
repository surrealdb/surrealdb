/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = """
	Plan companion to for_update_nested_targets: EXPLAIN plans a
	statement without running it, so the FOR UPDATE target check splits by
	when it fires. A target the planner can see is not a record id (a table,
	a literal value, a parameter already bound where the statement is
	planned) is refused at plan time, and EXPLAIN raises the same error
	wherever the SELECT sits. A parameter bound only when the enclosing block
	runs leaves a plan whose source is the parameter. A record-id target
	plans as the locked record lookup.
"""

[[test.results]]
value = "[{ id: person:1 }]"

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE requires record ids as targets"

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE requires record ids as targets"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE requires record ids as targets"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE requires record ids as targets"

[[test.results]]
value = '''
'Return [ctx: Db]
    Sequence [ctx: Db] [statements: 2]
        Let [ctx: Rt] [name: $r]
            Expr [ctx: Rt] [expr: [1]]
        SelectProject [ctx: Db] [projections: *]
            DynamicScan [ctx: Db] [source: $r, for_update: true]
''''

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE requires record ids as targets"

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE requires record ids as targets"

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE requires record ids as targets"

[[test.results]]
value = '''
'Return [ctx: Db]
    Project [ctx: Db]
        RecordIdScan [ctx: Db] [record_id: person:1, for_update: true]
''''
*/

CREATE person:1;

-- A table, nested.
EXPLAIN RETURN (SELECT * FROM person FOR UPDATE);
LET $p = SELECT * FROM person FOR UPDATE;

-- A parameter that resolves to a value, and to an array of record ids.
LET $p = [1];
EXPLAIN SELECT * FROM $p FOR UPDATE;
LET $q = [person:1];
EXPLAIN SELECT * FROM $q FOR UPDATE;

-- A parameter bound where the statement runs.
EXPLAIN RETURN { LET $r = [1]; SELECT * FROM $r FOR UPDATE };

-- Literal values, nested.
EXPLAIN RETURN (SELECT * FROM [1] FOR UPDATE);
EXPLAIN SELECT * FROM (SELECT * FROM [1] FOR UPDATE);
EXPLAIN RETURN (SELECT * FROM 'x' FOR UPDATE);

-- A record id is the one target the clause locks.
EXPLAIN RETURN (SELECT * FROM person:1 FOR UPDATE);
