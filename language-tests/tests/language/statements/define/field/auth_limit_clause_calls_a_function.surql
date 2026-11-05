/**
[env]
namespace = true
database = true
auth = { namespace = "test", database = "test", level = "editor" }

[test]
reason = """
A field defined by a database editor carries the editor's AUTH LIMIT, and its
clauses run for the writer narrowed by it. A call of a user-defined function
or a read statement in a VALUE, ASSERT, DEFAULT, write PERMISSIONS or COMPUTED
clause is made for that principal, and the field answers as any other: the
COMPUTED value derived for a later VALUE and the ASSERT refusing. A schema
statement in such a clause is refused where the field is defined (register
#28, region_constraints/value_ddl.surql).
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: src:1, n: 5 }]"

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
value = "[{ a: 4, b: 5, c: 3, d: 42, e: true, f: 8, g: 9, id: t:1 }]"

[[test.results]]
value = "[{ a: 8, b: 5, c: 3, d: 42, e: true, f: 16, g: 17, id: t:1 }]"

[[test.results]]
value = "[{ a: 8, b: 5, c: 3, d: 42, e: true, f: 16, g: 17, id: t:1 }]"

[[test.results]]
error = "Found 1 for field `c`, with record `t:2`, but field must conform to: fn::double($value) > 2"
*/

DEFINE FUNCTION fn::double($n: int) { RETURN $n * 2; };
CREATE src:1 SET n = 5;
DEFINE FIELD a ON t VALUE fn::double($value);
DEFINE FIELD b ON t VALUE (SELECT VALUE n FROM ONLY src:1);
DEFINE FIELD c ON t ASSERT fn::double($value) > 2;
DEFINE FIELD d ON t DEFAULT fn::double(21);
DEFINE FIELD e ON t PERMISSIONS FOR create, update WHERE fn::double(1) = 2;
DEFINE FIELD f ON t COMPUTED fn::double(a);
DEFINE FIELD g ON t VALUE f + 1;
CREATE t:1 SET a = 2, c = 3, e = true;
UPDATE t:1 SET a = 4;
SELECT * FROM t;
CREATE t:2 SET a = 1, c = 1;
