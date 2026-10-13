/**
[env]
imports = ["reproductions/a_return_in_a_permission_block_is_its_value_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:b" }

[test]
reason = """
A RETURN in a permission block is the permission's value. A block that ends in a RETURN, with or
without a LET beside it, grants the read when the value it returns is truthy and denies it when it
is not, by a scan and by a dereference alike. The value is computed with the predicate's unchecked
reads and never becomes the value of the statement the permission guards. A statement-level IF in
the block passes its RETURN to the block. A RETURN taken inside the predicate by a subquery's
projection, and a BREAK taken by a loop, are ordinary. A field permission a scan applies, and a
write's gate, take a RETURN as the verdict, and a field the verdict denies reads NONE. A COMPUTED
field and a field VALUE take a RETURN as their value, also where a permission derives the field, so
a permission reading a COMPUTED field that returns its value judges that value, as it judges one
computed with no RETURN. A table permission that reads a COMPUTED field reads the value the field
computes to (SECURITY_GUIDE.md, a PERMISSIONS predicate that reads a COMPUTED field). Every path
that judges a permission predicate takes the block's RETURN as the predicate's value.
"""

[[test.results]]
value = "[{ f: 1, id: gated4:1 }]"

[[test.results]]
value = "{ f: 1, id: gated4:1 }"

[[test.results]]
value = "1"

[[test.results]]
value = "[{ f: 1, id: gated5:1 }]"

[[test.results]]
value = "1"

[[test.results]]
value = "[{ f: 1, id: gated6:1 }]"

[[test.results]]
value = "1"

[[test.results]]
value = "{ f: 1 }"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[{ f: 1, id: gated7:1 }]"

[[test.results]]
value = "[{ f: 1, id: gated8:1 }]"

[[test.results]]
value = "[{ f: 1, id: gated9:1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ c: 2, d: 'early', f: 1, id: comp:1 }]"

[[test.results]]
value = "[{ f: 1, id: fq:1, t: 'T' }]"

[[test.results]]
value = "NONE"

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
