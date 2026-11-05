/**
[env]
namespace = true
database = true

[test]
reason = """
	Keeping a list is the engine's bookkeeping, not a write of the referenced
	record by the statement that changes a referencing one, so the EVENTs of the
	referenced table do not fire for it. A write of the referenced record by a
	statement still fires them, and sees the list as it stands.
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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: author:a, name: 'a', posts: [] }]"

[[test.results]]
value = "[{ author: author:a, id: post:1 }]"

[[test.results]]
value = "[{ author: author:a, id: post:2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[['CREATE', []]]"

[[test.results]]
value = "[{ id: author:a, name: 'b', posts: [post:2] }]"

[[test.results]]
value = "[['CREATE', []], ['UPDATE', [post:2]]]"
*/

DEFINE TABLE author SCHEMAFULL;
DEFINE FIELD name ON author TYPE string;
DEFINE FIELD posts ON author TYPE array<record<post>> DEFAULT [] READONLY;
DEFINE TABLE post SCHEMAFULL;
DEFINE FIELD author ON post TYPE record<author> REFERENCE MATERIALIZE INTO author.posts;
DEFINE TABLE log SCHEMALESS;
DEFINE EVENT author_changed ON author WHEN true THEN (CREATE log SET what = $event, posts = $after.posts);
CREATE author:a SET name = 'a';
CREATE post:1 SET author = author:a;
CREATE post:2 SET author = author:a;
DELETE post:1;
SELECT VALUE [what, posts] FROM log;
UPDATE author:a SET name = 'b';
SELECT VALUE [what, posts] FROM log ORDER BY what;
