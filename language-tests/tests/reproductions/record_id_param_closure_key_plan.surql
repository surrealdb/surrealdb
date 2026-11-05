/**
[env]

[test]
reason = """
	Plan companion to record_id_param_closure_key: a SELECT over a
	record-id parameter whose key holds a closure keeps the parameter as its
	source, so the plan shows a scan over the parameter rather than a
	catalog-resolved record lookup, at the top level and nested in a RETURN
	alike; the key is never serialized to produce the plan.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'Return [ctx: Rt]
    ExprPlan [ctx: Rt] [expr: (
  Project[*]
    Source[$r]
).len()]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Values [ctx: Rt] [expr: $r]
''''
*/

DEFINE TABLE t SCHEMALESS;

LET $r = t:{ f: || 1 };

EXPLAIN RETURN (SELECT * FROM $r).len();

EXPLAIN SELECT * FROM $r;
