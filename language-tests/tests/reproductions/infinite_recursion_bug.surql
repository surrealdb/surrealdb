/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "Computed field referencing $this.id.prop on an object-keyed RecordId should not infinite-loop via document fetch"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ foo: NONE, id: test:{ prop: 1 } }]"

[[test.results]]
value = "[{ foo: NONE, id: test:{ prop: 1 } }]"
*/

define table test;
define field foo on table test computed { return $this.id.prop };
create test:{ prop: 1 };
select * from test;
