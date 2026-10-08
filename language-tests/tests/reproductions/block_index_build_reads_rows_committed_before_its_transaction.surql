/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A `DEFINE INDEX` after a write of the same block builds over the rows
	committed before the statement's transaction began: the rows the block
	wrote before the definition are not entered, and stay out of the index
	once committed, and a unique index raises no error over them, while the
	writes after the definition enter theirs. An indexed read answers only
	those.
"""

[[test.results]]
value = "[item:2]"

[[test.results]]
value = "[{ id: dup:3 }]"

[[test.results]]
value = "[item:2, item:3]"
*/

{ CREATE item:1 SET k = 1; DEFINE INDEX by_k ON item FIELDS k; CREATE item:2 SET k = 1; RETURN SELECT VALUE id FROM item WHERE k = 1 ORDER BY id };
{ CREATE dup:1 SET k = 1; CREATE dup:2 SET k = 1; DEFINE INDEX u ON dup FIELDS k UNIQUE; CREATE dup:3 };
{ CREATE item:3 SET k = 1; RETURN SELECT VALUE id FROM item WHERE k = 1 ORDER BY id };
