/**
[env]
imports = ["reproductions/a_return_in_a_permission_block_is_an_error_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:b" }

[test]
reason = "A RETURN that leaves a table or field permission a read applies, with or without a LET beside it, is an error naming the stray control flow, by a scan and by a dereference alike: the value it carries, computed with the predicate's unchecked reads, never becomes the value of the statement the permission guards nor the read's verdict. A RETURN taken inside the predicate, by a subquery's projection, and a BREAK taken by a loop, are ordinary. A field permission a scan applies, and a write's gate, take a RETURN as the verdict. A COMPUTED field and a field VALUE take a RETURN as their value, also where a permission derives the field, so a permission reading a COMPUTED field that returns its value judges that value, as it judges one computed with no RETURN. Expected values are main's, except the last two: a table permission that reads a COMPUTED field reads the value the field computes to (SECURITY_GUIDE.md, a PERMISSIONS predicate that reads a COMPUTED field), where main judges the record as stored, reads NONE for the field and denies both records."

[[test.results]]
error = 'Internal database error: unexpected control flow in a permission predicate: RETURN String("SECRET")'

[[test.results]]
error = 'Internal database error: unexpected control flow in a permission predicate: RETURN String("SECRET")'

[[test.results]]
error = 'Internal database error: unexpected control flow in a permission predicate: RETURN String("SECRET")'

[[test.results]]
error = 'Internal database error: unexpected control flow in a permission predicate: RETURN String("SECRET")'

[[test.results]]
error = 'Internal database error: unexpected control flow in a permission predicate: RETURN String("SECRET")'

[[test.results]]
error = "Internal database error: unexpected control flow in a permission predicate: RETURN Bool(true)"

[[test.results]]
error = "Internal database error: unexpected control flow in a permission predicate: RETURN Bool(true)"

[[test.results]]
error = "Internal database error: unexpected control flow in a permission predicate: RETURN Bool(true)"

[[test.results]]
error = "Internal database error: unexpected control flow in a permission predicate: RETURN Bool(true)"

[[test.results]]
error = "Internal database error: unexpected control flow in a permission predicate: RETURN Bool(true)"

[[test.results]]
error = 'Internal database error: unexpected control flow in a permission predicate: RETURN String("x")'

[[test.results]]
value = "[{ f: 1, id: gated8:1 }]"

[[test.results]]
value = "[{ f: 1, id: gated9:1 }]"

[[test.results]]
error = "Internal database error: unexpected control flow in a permission predicate: RETURN Bool(false)"

[[test.results]]
error = "Internal database error: unexpected control flow in a permission predicate: RETURN Bool(false)"

[[test.results]]
value = "[{ c: 2, d: 'early', f: 1, id: comp:1 }]"

[[test.results]]
value = "[{ f: 1, id: fq:1, t: 'T' }]"

[[test.results]]
error = "Failed to check field permission: Internal database error: unexpected control flow in a permission predicate: RETURN Bool(false)"

[[test.results]]
value = "[{ s: NONE }]"

[[test.results]]
value = "[{ f: 1, id: fv:1, v: 6 }]"

[[test.results]]
value = "[{ f: 1, id: wg:1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ f: 2, id: wu:1 }]"

[[test.results]]
value = "[{ c: 2, f: 1, id: dv:1 }]"

[[test.results]]
value = "[{ c: 2, f: 1, id: dp:1 }]"
*/
SELECT * FROM gated4;
RETURN gated4:1.*;
RETURN gated4:1.f;
SELECT * FROM gated5;
RETURN gated5:1.f;
SELECT * FROM gated6;
RETURN gated6:1.f;
RETURN gated6:1.{f};
RETURN (SELECT VALUE f FROM gated6:1);
RETURN [gated6:1].f;
SELECT * FROM gated7;
SELECT * FROM gated8;
SELECT * FROM gated9;
SELECT * FROM gatedA;
RETURN gatedA:1.f;
SELECT * FROM comp;
SELECT * FROM fq;
RETURN fq:1.s;
SELECT s FROM fq;
CREATE fv:1 SET f = 1;
CREATE wg:1 SET f = 1;
CREATE wf:1 SET f = 1;
UPDATE wu:1 SET f = 2;
SELECT * FROM dv;
SELECT * FROM dp;
