/**
[env]
planner-strategy = ["all-ro"]

[test]

[[test.results]]
value = "[{ id: Test:1, name: 'my name' }]"

[[test.results]]
value = "[{ id: Test:2, ref: Test:1 }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: id, ref]
    Compute [ctx: Db] [fields: ref = ref.{ id }]
        SourceExpr [ctx: Db] [expr: [Test:1, Test:2]]
''''

[[test.results]]
value = "[{ id: Test:1, ref: NONE }, { id: Test:2, ref: { id: Test:1 } }]"
*/

CREATE Test:1 SET name="my name";
CREATE Test:2 SET ref=Test:1;

EXPLAIN SELECT id, ref.{id} FROM [Test:1, Test:2];
SELECT id, ref.{id} FROM [Test:1, Test:2];
