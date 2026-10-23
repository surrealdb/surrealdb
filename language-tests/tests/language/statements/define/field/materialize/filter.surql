/**
[env]
namespace = true
database = true

[test]
reason = """
	A MATERIALIZE INTO clause with a WHERE condition lists a record only while the
	condition holds for it: the record joins the list when it is created meeting
	the condition or when an update makes it meet it, leaves it when an update
	stops it meeting it, and is not listed when it never met it. Each list is
	compared with the references whose records meet the condition.
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
value = "[{ id: author:a, posts: [] }]"

[[test.results]]
value = "[{ archived: false, author: author:a, id: post:1 }]"

[[test.results]]
value = "[{ archived: true, author: author:a, id: post:2 }]"

[[test.results]]
value = "[{ archived: false, author: author:a, id: post:3 }]"

[[test.results]]
value = "[[post:1, post:3]]"

[[test.results]]
value = "[{ archived: true, author: author:a, id: post:1 }]"

[[test.results]]
value = "[[post:3]]"

[[test.results]]
value = "[{ archived: false, author: author:a, id: post:2 }]"

[[test.results]]
value = "[[post:2, post:3]]"

[[test.results]]
value = "[{ archived: true, author: author:a, id: post:3 }]"

[[test.results]]
value = "[{ archived: true, author: author:a, id: post:3 }]"

[[test.results]]
value = "[[post:2]]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[post:2]]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[]]"

[[test.results]]
value = "[]"
*/

DEFINE TABLE author SCHEMAFULL;
DEFINE FIELD posts ON author TYPE array<record<post>> DEFAULT [] READONLY;
DEFINE TABLE post SCHEMAFULL;
DEFINE FIELD archived ON post TYPE bool DEFAULT false;
DEFINE FIELD author ON post TYPE record<author> REFERENCE MATERIALIZE INTO author.posts WHERE archived != true;
CREATE author:a;
CREATE post:1 SET author = author:a;
CREATE post:2 SET author = author:a, archived = true;
CREATE post:3 SET author = author:a;
SELECT VALUE posts FROM author:a;
UPDATE post:1 SET archived = true;
SELECT VALUE posts FROM author:a;
UPDATE post:2 SET archived = false;
SELECT VALUE posts FROM author:a;
UPDATE post:3 SET archived = true;
UPDATE post:3 SET archived = true;
SELECT VALUE posts FROM author:a;
DELETE post:1;
SELECT VALUE posts FROM author:a;
DELETE post:2;
SELECT VALUE posts FROM author:a;
SELECT VALUE id FROM post WHERE author = author:a AND archived != true ORDER BY id;
