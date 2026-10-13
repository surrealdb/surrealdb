/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A VALUE clause preserves FLEXIBLE object contents; undeclared sub-keys are not stripped"
issue = 1088

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ data: { extra: 'survives', known: 1 }, id: t:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ data: { extra: 'survives', known: 1 }, id: u:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ data: { extra: 'survives', known: 1 }, id: v:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ data: { added: true, extra: 'survives', known: 1 }, id: w:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ data: { extra: 'survives', known: 1 }, id: x:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ data: [{ extra: 'survives', known: 1 }], id: y:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: z:1, outer: { inner: { extra: 'survives', known: 1 } } }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ data: { known: 1 }, id: m:1 }]"

[[test.results]]
value = "[{ data: { extra: 'survives', known: 2 }, id: m:1 }]"

[[test.results]]
value = "[{ data: { another: 'survives', extra: 'survives', known: 2 }, id: m:1 }]"
*/

-- FLEXIBLE object with a VALUE clause keeps undeclared sub-keys
DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD data ON TABLE t TYPE object FLEXIBLE VALUE $value;
CREATE t:1 SET data = { known: 1, extra: "survives" };

-- The same field without a VALUE clause behaves identically
DEFINE TABLE u SCHEMAFULL;
DEFINE FIELD data ON TABLE u TYPE object FLEXIBLE;
CREATE u:1 SET data = { known: 1, extra: "survives" };

-- A declared sub-field alongside a VALUE clause does not restrict the rest
DEFINE TABLE v SCHEMAFULL;
DEFINE FIELD data ON TABLE v TYPE object FLEXIBLE VALUE $value OR {};
DEFINE FIELD data.known ON TABLE v TYPE option<number>;
CREATE v:1 SET data = { known: 1, extra: "survives" };

-- A VALUE clause that rewrites the object keeps both its own and the input keys
DEFINE TABLE w SCHEMAFULL;
DEFINE FIELD data ON TABLE w TYPE object FLEXIBLE VALUE object::extend($value, { added: true });
CREATE w:1 SET data = { known: 1, extra: "survives" };

-- option<object> and array<object> follow the same rule
DEFINE TABLE x SCHEMAFULL;
DEFINE FIELD data ON TABLE x TYPE option<object> FLEXIBLE VALUE $value;
CREATE x:1 SET data = { known: 1, extra: "survives" };

DEFINE TABLE y SCHEMAFULL;
DEFINE FIELD data ON TABLE y TYPE array<object> FLEXIBLE VALUE $value;
CREATE y:1 SET data = [{ known: 1, extra: "survives" }];

-- FLEXIBLE on a nested field under a parent carrying the VALUE clause
DEFINE TABLE z SCHEMAFULL;
DEFINE FIELD outer ON TABLE z TYPE object VALUE $value;
DEFINE FIELD outer.inner ON TABLE z TYPE object FLEXIBLE;
CREATE z:1 SET outer = { inner: { known: 1, extra: "survives" } };

-- CONTENT and MERGE writes through the VALUE clause preserve sub-keys too
DEFINE TABLE m SCHEMAFULL;
DEFINE FIELD data ON TABLE m TYPE object FLEXIBLE VALUE $value;
CREATE m:1 SET data = { known: 1 };
UPDATE m:1 CONTENT { data: { known: 2, extra: "survives" } };
UPDATE m:1 MERGE { data: { another: "survives" } };
