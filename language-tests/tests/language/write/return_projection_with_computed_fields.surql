/**
[env]
namespace = true
database = true

[test]
reason = """
A `RETURN` clause reads the pre-mutation view, the post-mutation view, or both,
and only one of those exists on a statement that creates or deletes a record. A
`COMPUTED` field cannot be derived from a view that holds no record — the
expression fails on the absent input rather than yielding NONE — so the missing
view is left as it is.

Every form here is run twice, against a table that has a computed field and a
table that does not, because the two must answer identically except where the
computed field is both derivable and asked for. A computed field is the only
thing that distinguishes the two schemas, so any difference beyond that is the
missing view being derived from nothing.

`RETURN BEFORE` on a delete is where the computed field legitimately shows up:
the record being removed still has an `a` to derive it from.
"""

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
value = "[{ a: 5 }]"

[[test.results]]
value = "[{ a: 5 }]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[[{ op: 'replace', path: '', value: { a: 5, dbl: 10, id: b:c3 } }]]"

[[test.results]]
value = "[[{ op: 'replace', path: '', value: { a: 5, id: n:c3 } }]]"

[[test.results]]
value = "[{ a: 5 }]"

[[test.results]]
value = "[{ a: 5 }]"

[[test.results]]
value = "[{ a: 5 }]"

[[test.results]]
value = "[{ a: 5 }]"

[[test.results]]
value = "[{ dbl: 10 }]"

[[test.results]]
value = "[{ a: 5, dbl: 10 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 6 }]"

[[test.results]]
value = "[{ a: 6 }]"

[[test.results]]
value = "[{ a: 6, dbl: 12, id: b:u1 }]"

[[test.results]]
value = "[{ a: 6, id: n:u1 }]"

[[test.results]]
value = "[[{ op: 'replace', path: '/a', value: 8 }, { op: 'replace', path: '/dbl', value: 16 }]]"

[[test.results]]
value = "[[{ op: 'replace', path: '/a', value: 8 }]]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: NONE }]"

[[test.results]]
value = "[{ a: NONE }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 5, dbl: 10, id: b:d3 }]"

[[test.results]]
value = "[{ a: 5, id: n:d3 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[{ op: 'replace', path: '', value: NONE }]]"

[[test.results]]
value = "[[{ op: 'replace', path: '', value: NONE }]]"
*/

DEFINE TABLE b SCHEMALESS;
DEFINE FIELD a   ON b TYPE number;
DEFINE FIELD dbl ON b COMPUTED a * 2;
DEFINE TABLE n SCHEMALESS;
DEFINE FIELD a ON n TYPE number;

-- Creating shapes: no pre-mutation view.
CREATE b:c1 SET a = 5 RETURN a;
CREATE n:c1 SET a = 5 RETURN a;
CREATE b:c2 SET a = 5 RETURN BEFORE;
CREATE n:c2 SET a = 5 RETURN BEFORE;
CREATE b:c3 SET a = 5 RETURN DIFF;
CREATE n:c3 SET a = 5 RETURN DIFF;
UPSERT b:c4 SET a = 5 RETURN a;
UPSERT n:c4 SET a = 5 RETURN a;
INSERT INTO b { id: b:c5, a: 5 } RETURN a;
INSERT INTO n { id: n:c5, a: 5 } RETURN a;

-- A computed field asked for by name on a creating shape is derivable, because
-- the post-mutation view is the one that exists.
CREATE b:c6 SET a = 5 RETURN dbl;
CREATE b:c7 SET a = 5 RETURN a, dbl;

-- Updating shapes: both views exist, so nothing is skipped.
CREATE b:u1 SET a = 5 RETURN NONE;
CREATE n:u1 SET a = 5 RETURN NONE;
UPDATE b:u1 SET a = 6 RETURN a;
UPDATE n:u1 SET a = 6 RETURN a;
UPDATE b:u1 SET a = 7 RETURN BEFORE;
UPDATE n:u1 SET a = 7 RETURN BEFORE;
UPDATE b:u1 SET a = 8 RETURN DIFF;
UPDATE n:u1 SET a = 8 RETURN DIFF;

-- Deleting shapes: no post-mutation view.
CREATE b:d1 SET a = 5 RETURN NONE;
CREATE n:d1 SET a = 5 RETURN NONE;
DELETE b:d1 RETURN a;
DELETE n:d1 RETURN a;
CREATE b:d2 SET a = 5 RETURN NONE;
CREATE n:d2 SET a = 5 RETURN NONE;
DELETE b:d2 RETURN AFTER;
DELETE n:d2 RETURN AFTER;
CREATE b:d3 SET a = 5 RETURN NONE;
CREATE n:d3 SET a = 5 RETURN NONE;
DELETE b:d3 RETURN BEFORE;
DELETE n:d3 RETURN BEFORE;
CREATE b:d4 SET a = 5 RETURN NONE;
CREATE n:d4 SET a = 5 RETURN NONE;
DELETE b:d4 RETURN DIFF;
DELETE n:d4 RETURN DIFF;
