/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Verify that $this.field, $self.field, $self[\"field\"], function args, and nested parens all resolve correctly for computed field evaluation ordering"

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

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 42, b: 84, c: 84, d: 84, e: 210, f: 210, g: 210, id: tb1:test }]"

[[test.results]]
value = "[{ a: 42, b: 84, c: 84, d: 84, e: 210, f: 210, g: 210, id: tb1:test }]"

*/

DEFINE TABLE tb1 SCHEMAFULL;
DEFINE FIELD a ON tb1 COMPUTED 42;
DEFINE FIELD b ON tb1 COMPUTED a * 2;
DEFINE FIELD c ON tb1 COMPUTED $this.a * 2;
DEFINE FIELD d ON tb1 COMPUTED $self.a * 2;
DEFINE FIELD e ON tb1 COMPUTED math::sum([$this.a, b, $self.c]);
DEFINE FIELD f ON tb1 COMPUTED (a + ($this.b + $self.c));
DEFINE FIELD g ON tb1 COMPUTED (a + ($this.b + $self["c"]));

UPSERT tb1:test;
SELECT * FROM tb1;
