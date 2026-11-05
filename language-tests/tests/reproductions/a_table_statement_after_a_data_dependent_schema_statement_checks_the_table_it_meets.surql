/**
[env]
backend = ["mem", "surrealkv"]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A DEFINE TABLE or REMOVE TABLE after a schema statement whose effect depends
	on the data judges the table it meets when it runs: removing a view the
	block defined forgets it on the tables it reads, removing a LIGHTWEIGHT
	relation or one of its endpoints is refused while the relation holds the
	edges the block wrote, and redefining a relation the block gave an INLINE
	field, or a table it gave a field as LIGHTWEIGHT, is refused.
"""

[[test.results]]
value = "[{ id: flag:1, on: true }]"

[[test.results]]
value = "[{ id: src:1, n: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: src:2, n: 2 }]"

[[test.results]]
value = "[{ id: a:1 }]"

[[test.results]]
value = "[{ id: b:1 }]"

[[test.results]]
error = "An error occurred: cannot remove the LIGHTWEIGHT relation `e` while it holds edges: delete them first (e.g. `DELETE e`)"

[[test.results]]
error = "An error occurred: cannot remove `a`: the LIGHTWEIGHT relation `e2` uses it as an endpoint and still holds edges — delete them first (e.g. `DELETE e2`)"

[[test.results]]
error = "An error occurred: the relation `w` still has INLINE fields; remove or un-inline them before changing its type"

[[test.results]]
error = "An error occurred: only a table without fields, events or indexes can become a LIGHTWEIGHT relation: its edges store no records for them to apply to"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['a', 'b', 'flag', 'src', 'y']"
*/
CREATE flag:1 SET on = true;
CREATE src:1 SET n = 1;
{ IF (SELECT VALUE on FROM ONLY flag:1) { DEFINE TABLE v AS SELECT count() AS c FROM src GROUP ALL }; REMOVE TABLE v };
object::keys((INFO FOR TABLE src).tables);
CREATE src:2 SET n = 2;
CREATE a:1; CREATE b:1;
{ IF (SELECT VALUE on FROM ONLY flag:1) { DEFINE TABLE e TYPE RELATION FROM a TO b LIGHTWEIGHT }; RELATE a:1->e->b:1; REMOVE TABLE e };
{ IF (SELECT VALUE on FROM ONLY flag:1) { DEFINE TABLE e2 TYPE RELATION FROM a TO b LIGHTWEIGHT }; RELATE a:1->e2->b:1; REMOVE TABLE a };
{ IF (SELECT VALUE on FROM ONLY flag:1) { DEFINE TABLE w TYPE RELATION FROM a TO b }; DEFINE FIELD f ON w TYPE int INLINE; DEFINE TABLE OVERWRITE w TYPE NORMAL };
{ IF (SELECT VALUE on FROM ONLY flag:1) { DEFINE TABLE x TYPE RELATION FROM a TO b }; DEFINE FIELD g ON x TYPE int; DEFINE TABLE OVERWRITE x TYPE RELATION FROM a TO b LIGHTWEIGHT };
{ IF (SELECT VALUE on FROM ONLY flag:1) { DEFINE TABLE y TYPE RELATION FROM a TO b LIGHTWEIGHT }; DEFINE TABLE OVERWRITE y TYPE RELATION FROM a TO b };
object::keys((INFO FOR DB).tables);
