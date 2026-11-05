/**
[test]
reason = """
	An edge is stored as its record, under its own table, and as a pointer on
	each endpoint, under the endpoint's table. REMOVE TABLE deletes the table's
	own keyspace and purges the pointers its edges left on their endpoints, so a
	traversal from an endpoint reaches no removed edge, and a table defined
	again under the name holds only the edges written to it since: a reused edge
	id is listed once, and a hop reaches only the vertex its stored edge names.
	The purge covers an edge table of any type (an implicitly defined table is
	TYPE ANY), clears an endpoint's inline edge cache, honours AND EXPUNGE, and
	leaves the edges of every other table in place.
"""

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
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[], [], []]"

[[test.results]]
value = "[[], [], []]"

[[test.results]]
value = "[[]]"

[[test.results]]
value = "[[]]"

[[test.results]]
value = "[[]]"

[[test.results]]
value = "[[p:2]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: k:e1, in: p:1, out: p:3 }]"

[[test.results]]
value = "[[k:e1]]"

[[test.results]]
value = "[[p:3]]"

[[test.results]]
value = "[[], [], [p:1]]"

[[test.results]]
value = "[[p:2]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[a:1]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[]]"

[[test.results]]
value = "[[]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[a:2]]"

[[test.results]]
value = "[[v:2]]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[]]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[s:e]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[]]"

[[test.results]]
value = "[[]]"

[[test.results]]
value = "[{ id: w:x }]"

*/

-- A relation whose edges leave a hub, enter it, and share it with another relation.
DEFINE TABLE p SCHEMALESS;
DEFINE TABLE k TYPE RELATION IN p OUT p;
DEFINE TABLE j TYPE RELATION IN p OUT p;
CREATE p:1, p:2, p:3 RETURN NONE;
RELATE p:1->k:e1->p:2 RETURN NONE;
RELATE p:1->k:e2->p:3 RETURN NONE;
RELATE p:2->k:e3->p:1 RETURN NONE;
RELATE p:1->j:x->p:2 RETURN NONE;
REMOVE TABLE k;
SELECT VALUE ->k FROM p ORDER BY id;
SELECT VALUE <-k FROM p ORDER BY id;
SELECT VALUE ->k->p FROM p:1;
SELECT VALUE <-k<-p FROM p:2;
SELECT VALUE <->k FROM p:1;
SELECT VALUE ->j->p FROM p:1;

-- Defined again, the table holds only what is written to it since.
DEFINE TABLE k TYPE RELATION IN p OUT p;
RELATE p:1->k:e1->p:3 RETURN NONE;
SELECT * FROM k;
SELECT VALUE ->k FROM p:1;
SELECT VALUE ->k->p FROM p:1;
SELECT VALUE <-k<-p FROM p ORDER BY id;
SELECT VALUE ->j->p FROM p:1;

-- An implicitly defined edge table, an endpoint with an inline edge cache, and AND EXPUNGE.
DEFINE TABLE v SCHEMALESS INLINE EDGES 8;
CREATE v:1, v:2 RETURN NONE;
RELATE v:1->a:1->v:2 RETURN NONE;
SELECT VALUE ->a FROM v:1;
REMOVE TABLE AND EXPUNGE a;
SELECT VALUE ->a FROM v:1;
SELECT VALUE <-a<-v FROM v:2;
DEFINE TABLE a SCHEMALESS;
RELATE v:2->a:2->v:1 RETURN NONE;
SELECT VALUE ->a FROM v:2;
SELECT VALUE <-a<-v FROM v:1;

-- An edge table whose edges join its own records: everything it held goes with it.
CREATE s:a, s:b RETURN NONE;
RELATE s:a->s:e->s:b RETURN NONE;
REMOVE TABLE s;
CREATE s:a, s:b RETURN NONE;
SELECT VALUE ->s FROM s:a;
RELATE s:a->s:e->s:b RETURN NONE;
SELECT VALUE ->s FROM s:a;

-- A table defined again as a view is emptied of its edges before the view fills it.
DEFINE TABLE w TYPE ANY;
RELATE p:1->w:1->p:2 RETURN NONE;
DEFINE TABLE OVERWRITE w AS SELECT * FROM j;
SELECT VALUE ->w FROM p:1;
SELECT VALUE <-w<-p FROM p:2;
SELECT id FROM w;
