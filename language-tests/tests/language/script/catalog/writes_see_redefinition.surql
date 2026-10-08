/**
[test]
reason = "A write calls each user-defined function as the catalog stands at the call: a function an embedded script redefines earlier in the same row is called with its new body, in a SET and in a statement nested in it."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1 }, { id: t:2 }]"

[[test.results]]
value = "[{ a: 2, c: 2, id: t:1 }, { a: 2, c: 2, id: t:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 2, id: t:3 }]"
*/
DEFINE FUNCTION fn::f() { RETURN 1 };
DEFINE FUNCTION fn::w() { RETURN function() { return surrealdb.query("DEFINE FUNCTION OVERWRITE fn::f() { RETURN 2 }") } };
DEFINE FUNCTION fn::r() { RETURN function() { return surrealdb.query("DEFINE FUNCTION OVERWRITE fn::f() { RETURN 1 }") } };
CREATE t:1, t:2;
UPDATE t SET b = fn::w(), a = fn::f(), c = (RETURN fn::f());
fn::r();
CREATE t:3 SET b = fn::w(), a = fn::f();
