/**
[env]
imports = ["reproductions/a_late_bound_statement_import.surql"]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "As a_computed_field_is_derived_after_the_fields_it_reads, each statement late-bound: planned after a schema statement the plan cannot know the effect of, so it loads its tables' permissions and computed fields when it opens its source."

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

# A write's output is derived by the document pipeline on every strategy.
[[test.results]]
value = "[{ a: 100, b: 10, c: 1, can_edit: false, id: t:1, is_locked: true, n: 1, status: 'locked' }]"

[[test.results]]
value = "[{ a: 200, b: 20, c: 2, can_edit: false, id: t:1, is_locked: true, n: 2, status: 'locked' }]"

# The read path, projected and whole.
[[test.results]]
value = "[{ a: 200, can_edit: false }]"

[[test.results]]
value = "[{ a: 200, b: 20, c: 2, can_edit: false, id: t:1, is_locked: true, n: 2, status: 'locked' }]"

[[test.results]]
value = "[false]"

*/

DEFINE FIELD a ON t COMPUTED b * 10;
DEFINE FIELD b ON t COMPUTED c * 10;
DEFINE FIELD c ON t COMPUTED n;
DEFINE FIELD is_locked ON t COMPUTED status = 'locked';
DEFINE FIELD can_edit ON t COMPUTED !is_locked;

{ LET $late = fn::late('now'); RETURN CREATE t:1 SET n = 1, status = 'locked'; };
{ LET $late = fn::late('now'); RETURN UPDATE t:1 SET n = 2; };

{ LET $late = fn::late('now'); RETURN SELECT a, can_edit FROM t:1; };
{ LET $late = fn::late('now'); RETURN SELECT * FROM t:1; };
{ LET $late = fn::late('now'); RETURN SELECT VALUE can_edit FROM t WHERE can_edit = false; };
