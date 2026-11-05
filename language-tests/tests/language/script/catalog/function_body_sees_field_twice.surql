/**
[test]
requires = ["scripting"]
reason = "A function body that defines a field through an embedded script and then writes applies the field; called again it fails on the record it wrote."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ f: 42, id: u:1 }]"

[[test.results]]
error = "Database record `u:1` already exists"
*/
DEFINE FUNCTION fn::x() { RETURN function() { return surrealdb.query("DEFINE FIELD OVERWRITE f ON u VALUE 42") } };
DEFINE FUNCTION fn::y() { fn::x(); CREATE u:1; RETURN SELECT * FROM u; };
fn::y();
fn::y();
