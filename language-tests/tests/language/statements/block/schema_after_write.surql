/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A schema statement of a block after a write of the same block that defined
	its table finds the table the write defined: a `DEFINE FIELD` or
	`DEFINE EVENT` extends it, whether the write ran or a condition skipped it,
	and the statements after it read the definition it leaves. A view defined
	after a write to its source is filled from the source and maintained by the
	writes after it. A `DEFINE TABLE` of a table a write already defined
	raises, and the block's writes are undone.
"""

[[test.results]]
value = "[{ id: fresh:1, n: 1 }, { id: fresh:2, m: 5, n: 2 }]"

[[test.results]]
value = "[{ id: log:2, of: watched:2 }]"

[[test.results]]
value = "[{ id: later:2, m: 5 }]"

[[test.results]]
value = "[{ n: 7 }, { n: 8 }]"

[[test.results]]
error = "The table 'twice' already exists"

[[test.results]]
error = "The table 'twice' does not exist"
*/

{ CREATE fresh:1 SET n = 1; DEFINE FIELD m ON fresh VALUE 5; CREATE fresh:2 SET n = 2; RETURN SELECT * FROM fresh ORDER BY id };
{ CREATE watched:1; DEFINE EVENT noted ON watched THEN (UPSERT type::record('log', record::id($after.id)) SET of = $after.id); CREATE watched:2; RETURN SELECT * FROM log };
{ IF (SELECT VALUE n FROM ONLY fresh:1) = 0 { CREATE later:1 }; DEFINE FIELD m ON later VALUE 5; CREATE later:2; RETURN SELECT * FROM later ORDER BY id };
{ CREATE src:1 SET n = 7; DEFINE TABLE mirror AS SELECT n FROM src; CREATE src:2 SET n = 8; RETURN SELECT n FROM mirror ORDER BY n };
{ CREATE twice:1; DEFINE TABLE twice; CREATE twice:2 };
SELECT * FROM twice;
