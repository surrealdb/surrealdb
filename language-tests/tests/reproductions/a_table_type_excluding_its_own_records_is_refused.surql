/**
[test]
reason = """
	A write stores a record only in a table whose type admits its kind, and a
	record keeps its kind: an edge stays an edge and anything else stays a
	record that is no edge. So DEFINE TABLE OVERWRITE and ALTER TABLE refuse
	TYPE RELATION while the table holds a record that is no edge, and TYPE
	NORMAL while it holds an edge. A table whose type excluded one of its own
	records would hold one no write could have stored, which its export writes
	back by its own kind and its import refuses. TYPE ANY admits both kinds and
	is always accepted, and a change is accepted once the records of the other
	kind are gone. A record holding `in` and `out` fields is still no edge.
	IN, OUT and ENFORCED constrain the edges written after them, as a field's
	type constrains the values written after it, so they are not judged against
	the edges a relation already holds.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "Cannot make `t` TYPE RELATION: it holds `t:1`, which is not an edge. Delete the record first, or define the table TYPE ANY"

[[test.results]]
error = "Cannot make `t` TYPE RELATION: it holds `t:1`, which is not an edge. Delete the record first, or define the table TYPE ANY"

[[test.results]]
error = "Cannot make `t` TYPE RELATION: it holds `t:1`, which is not an edge. Delete the record first, or define the table TYPE ANY"

[[test.results]]
value = "'DEFINE TABLE t TYPE ANY SCHEMALESS PERMISSIONS NONE'"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "Cannot make `e` TYPE NORMAL: it holds the edge `e:1`. Delete the edge first, or define the table TYPE ANY"

[[test.results]]
error = "Cannot make `e` TYPE NORMAL: it holds the edge `e:1`. Delete the edge first, or define the table TYPE ANY"

[[test.results]]
value = "'DEFINE TABLE e TYPE RELATION SCHEMALESS PERMISSIONS NONE'"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Cannot make `m` TYPE RELATION: it holds `m:1`, which is not an edge. Delete the record first, or define the table TYPE ANY"

[[test.results]]
error = "Cannot make `m` TYPE NORMAL: it holds the edge `m:2`. Delete the edge first, or define the table TYPE ANY"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'DEFINE TABLE e TYPE ANY SCHEMALESS PERMISSIONS NONE'"

[[test.results]]
value = "'DEFINE TABLE t TYPE ANY SCHEMALESS PERMISSIONS NONE'"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Cannot make `ln` TYPE RELATION: it holds `ln:2`, which is not an edge. Delete the record first, or define the table TYPE ANY"

[[test.results]]
value = "[{ id: ln:2, in: hub:1, out: person:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Couldn't coerce value for field `in` of `rel:2`: Expected `record<person>` but found `hub:1`"

[[test.results]]
value = "[{ id: rel:1, in: hub:1, out: person:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "Cannot make `q` TYPE RELATION: it holds `q:1`, which is not an edge. Delete the record first, or define the table TYPE ANY"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
error = "The table 'q' does not exist"

*/

-- A record that is no edge keeps a table from becoming a relation.
DEFINE TABLE t SCHEMALESS;
CREATE t:1 SET a = 1 RETURN NONE;
ALTER TABLE t TYPE RELATION;
DEFINE TABLE OVERWRITE t TYPE RELATION;
DEFINE TABLE OVERWRITE t TYPE RELATION IN a OUT b ENFORCED;
(INFO FOR DB).tables.t;
CREATE t:2 RETURN NONE;

-- An edge keeps a table from becoming NORMAL.
DEFINE TABLE e TYPE RELATION SCHEMALESS;
RELATE a:1->e:1->b:1 RETURN NONE;
ALTER TABLE e TYPE NORMAL;
DEFINE TABLE OVERWRITE e TYPE NORMAL;
(INFO FOR DB).tables.e;
RELATE a:1->e:2->b:1 RETURN NONE;

-- A table holding both kinds can only be TYPE ANY.
DEFINE TABLE m TYPE ANY;
CREATE m:1 RETURN NONE;
RELATE a:1->m:2->b:1 RETURN NONE;
ALTER TABLE m TYPE RELATION;
ALTER TABLE m TYPE NORMAL;
ALTER TABLE e TYPE ANY;
DEFINE TABLE OVERWRITE t TYPE ANY;
(INFO FOR DB).tables.e;
(INFO FOR DB).tables.t;

-- Once the records of the other kind are gone the change is accepted.
DELETE m:1;
ALTER TABLE m TYPE RELATION;
DELETE t;
ALTER TABLE t TYPE RELATION;
ALTER TABLE e TYPE RELATION;

-- A record holding `in` and `out` is still no edge.
DEFINE TABLE person SCHEMALESS;
CREATE person:1, person:2 RETURN NONE;
CREATE ln:2 SET in = hub:1, out = person:2 RETURN NONE;
DEFINE TABLE OVERWRITE ln TYPE RELATION IN person OUT person ENFORCED;
SELECT * FROM ln;

-- IN, OUT and ENFORCED constrain the edges written after them.
DEFINE TABLE rel TYPE RELATION SCHEMALESS;
RELATE hub:1->rel:1->person:2 RETURN NONE;
DEFINE TABLE OVERWRITE rel TYPE RELATION IN person OUT person ENFORCED;
RELATE hub:1->rel:2->person:2 RETURN NONE;
SELECT * FROM rel;

-- A refusal inside a transaction cancels it.
BEGIN;
CREATE q:1 RETURN NONE;
ALTER TABLE q TYPE RELATION;
COMMIT;
SELECT * FROM q;
