/**
[env]
namespace = true
database = true

[test]
reason = "In the clauses of a write statement and of the field definitions, events and RETURN projection it evaluates, an unbound $parent is NONE rather than the record being written; a statement nested in the write binds $parent to that record, and a write nested in a SELECT reads the SELECT row as $parent."

[[test.results]]
value = "[{ a: 1, id: t:1 }]"

[[test.results]]
value = "[{ a: 2, id: t:2 }]"

[[test.results]]
value = "[{ a: 1, id: t:1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[NONE, NONE]"

[[test.results]]
value = "[{ id: t:3 }]"

[[test.results]]
value = "[{ id: t:4 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: t:2, u: [2] }]"

[[test.results]]
value = "[{ id: t:2, u: [t:2] }]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[{ id: t:5 }]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[2]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[7]"

[[test.results]]
value = "[]"

[[test.results]]
error = "The table 'u' does not exist"

[[test.results]]
error = "The table 'w' does not exist"

[[test.results]]
value = "[{ a: 1, id: u:1 }]"

[[test.results]]
value = "[{ a: 7, id: w:1 }]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[[1]]"

[[test.results]]
value = "[[7]]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[[1]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[1]]"

[[test.results]]
value = "[[NONE, 1]]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[[1]]"

[[test.results]]
value = "[[1]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, fc: NONE, id: v:1 }]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[{ a: 1, pa: NONE }]"

[[test.results]]
value = "[7]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, id: pt:1 }]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[[7]]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[1, 1]]"

[[test.results]]
value = "[{ a: 5, fc: NONE, id: v:2 }]"

[[test.results]]
value = "[{ a: 5, fc: NONE, id: v:2, z: 7 }]"

*/
CREATE t:1 SET a = 1;
CREATE t:2 SET a = 2;
UPDATE t:1 SET c = $parent RETURN AFTER;
UPDATE t WHERE $parent RETURN VALUE id;
UPDATE t SET d = $parent.a RETURN VALUE d;
CREATE t:3 SET e = $parent RETURN AFTER;
UPSERT t:4 SET e = $parent RETURN AFTER;
DELETE t WHERE $parent RETURN BEFORE;
SELECT id, (UPDATE t:1 SET f = $parent.a RETURN VALUE f) AS u FROM t:2;
SELECT id, (UPDATE t:1 SET g = $parent.id RETURN VALUE g) AS u FROM t:2;
RETURN (UPDATE t:1 SET h = $parent RETURN VALUE h);
INSERT INTO t { id: t:5, p: $parent } RETURN AFTER;
RELATE t:1->r->t:2 SET p = $parent RETURN VALUE p;
UPDATE t:2 SET q = $this.a RETURN VALUE q;
LET $parent = 7;
UPDATE t:1 SET k = $parent RETURN VALUE k;
DELETE t;
DELETE u;
DELETE w;
CREATE u:1 SET a = 1;
CREATE w:1 SET a = 7;
UPDATE u:1 SET c = (SELECT VALUE $parent.a FROM ONLY w:1) RETURN VALUE c;
UPDATE u:1 SET c = (SELECT VALUE $parent.a FROM w) RETURN VALUE c;
UPDATE u:1 SET c = (SELECT VALUE a FROM w WHERE $parent.a = 1) RETURN VALUE c;
RETURN (SELECT VALUE $parent.a FROM w);
SELECT VALUE (SELECT VALUE $parent.a FROM w) FROM u:1;
DEFINE FUNCTION fn::p() { RETURN (SELECT VALUE $parent.a FROM w) };
UPDATE u:1 SET c = fn::p() RETURN VALUE c;
UPDATE u:1 SET c = $parent.a, d = (SELECT VALUE $parent.a FROM ONLY w:1) RETURN VALUE [c, d];
CREATE w:2 SET a = 9, b = (SELECT VALUE $parent.b FROM ONLY u:1) RETURN VALUE b;
UPDATE u:1 SET e = (UPDATE w:1 SET z = $parent.a RETURN VALUE z) RETURN VALUE e;
UPDATE u:1 SET f = (CREATE x SET z = $parent.a RETURN VALUE z) RETURN VALUE f;
DEFINE FIELD fv ON v VALUE $parent.a;
DEFINE FIELD fd ON v DEFAULT $parent.a;
DEFINE FIELD fc ON v COMPUTED $parent.a;
DEFINE EVENT ev ON v THEN { CREATE vlog SET p = $parent.a };
CREATE v:1 SET a = 1 RETURN AFTER;
SELECT VALUE p FROM vlog;
UPDATE v:1 SET b = 2 RETURN $parent.a AS pa, a;
UPDATE v:1 SET b = 3 RETURN VALUE $parent;
DEFINE TABLE pt PERMISSIONS FOR create, update WHERE $parent.a = 1 FOR select FULL;
CREATE pt:1 SET a = 1;
UPDATE v SET c = (SELECT VALUE a FROM ONLY $parent.id) RETURN VALUE c;
UPDATE v SET c2 = (SELECT VALUE $parent.a FROM ONLY v:1) RETURN VALUE c2;
UPDATE v SET c3 = [1].map(|$x| $parent) RETURN VALUE c3;
UPDATE v SET c4 = a WHERE $parent.a = 1 RETURN VALUE c4;
UPDATE v SET c5 = $this.a, c6 = @.a RETURN VALUE [c5, c6];
INSERT INTO v { id: v:2, a: 5 } ON DUPLICATE KEY UPDATE z = $parent RETURN AFTER;
INSERT INTO v { id: v:2, a: 5 } ON DUPLICATE KEY UPDATE z = $parent RETURN AFTER;
