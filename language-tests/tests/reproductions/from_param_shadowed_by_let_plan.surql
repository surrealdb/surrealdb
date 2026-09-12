/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = """
	Plan companion to from_param_shadowed_by_let: a SELECT whose FROM
	names a parameter keeps the parameter as its source wherever the binding
	is made inside the statement's own block or by a FOR loop variable, so the
	block's LET or the loop stands above a scan over the parameter. At the
	top level the enclosing binding is in effect where the SELECT is planned,
	so a table-valued parameter plans as a scan of that table and a record-id
	parameter as a lookup of that record.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:1, kind: 'person' }]"

[[test.results]]
value = "[{ id: company:1, kind: 'company' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
"Return [ctx: Db]
    Sequence [ctx: Db] [statements: 2]
        Let [ctx: Rt] [name: $tb]
            Expr [ctx: Rt] [expr: company]
        ProjectValue [ctx: Db] [expr: kind]
            DynamicScan [ctx: Db] [source: $tb]
"'''

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'Return [ctx: Db]
    Sequence [ctx: Db] [statements: 2]
        Let [ctx: Rt] [name: $r]
            Expr [ctx: Rt] [expr: company:1]
        ProjectValue [ctx: Db] [expr: kind]
            DynamicScan [ctx: Db] [source: $r]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: kind]
    TableScan [ctx: Db] [table: person, direction: Forward]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: kind]
    RecordIdScan [ctx: Db] [record_id: person:1]
''''

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
"Return [ctx: Db]
    Sequence [ctx: Db] [statements: 2]
        Foreach [ctx: Db] [param: $loop, range: [company]]
            Return [ctx: Db]
                ProjectValue [ctx: Db] [expr: kind]
                    DynamicScan [ctx: Db] [source: $loop]
        Return [ctx: Rt]
            Expr [ctx: Rt] [expr: 'unreached']
"'''

[[test.results]]
value = '''
'Return [ctx: Db]
    Sequence [ctx: Db] [statements: 2]
        Let [ctx: Rt] [name: $n]
            Expr [ctx: Rt] [expr: NONE]
        SelectProject [ctx: Db] [projections: *]
            DynamicScan [ctx: Db] [source: $n]
''''
*/

DEFINE TABLE person SCHEMALESS;
DEFINE TABLE company SCHEMALESS;
CREATE person:1 SET kind = 'person';
CREATE company:1 SET kind = 'company';

LET $tb = type::table('person');

EXPLAIN RETURN { LET $tb = type::table('company'); SELECT VALUE kind FROM $tb };

LET $r = person:1;

EXPLAIN RETURN { LET $r = company:1; SELECT VALUE kind FROM $r };

-- The enclosing binding is untouched by the inner LET.
EXPLAIN SELECT VALUE kind FROM $tb;

EXPLAIN SELECT VALUE kind FROM $r;

-- A FOR loop variable used as a source is bound per iteration.
LET $loop = type::table('person');

EXPLAIN RETURN { FOR $loop IN [type::table('company')] { RETURN SELECT VALUE kind FROM $loop }; RETURN 'unreached' };

-- A parameter bound to NONE where the SELECT runs is no rows, as a literal NONE source is.
EXPLAIN RETURN { LET $n = NONE; SELECT * FROM $n };
