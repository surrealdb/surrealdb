/**
[env]
namespace = true
database = true

[test]
reason = """
	The destination of a MATERIALIZE INTO clause is kept by the engine, so no
	statement of a user changes it: SET, MERGE and a CONTENT or REPLACE that
	names a different list are refused, and a CONTENT or REPLACE that leaves
	the field out keeps the list the record holds. Writing the other fields of
	the record is unaffected.
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
value = "[{ id: author:a, name: 'a', posts: [] }]"

[[test.results]]
value = "[{ author: author:a, id: post:1 }]"

[[test.results]]
error = "Found changed value for field `posts`, with record `author:a`, but field is readonly"

[[test.results]]
error = "Found changed value for field `posts`, with record `author:a`, but field is readonly"

[[test.results]]
error = "Found changed value for field `posts`, with record `author:a`, but field is readonly"

[[test.results]]
error = "Found changed value for field `posts`, with record `author:a`, but field is readonly"

[[test.results]]
value = "[{ id: author:a, name: 'b', posts: [post:1] }]"

[[test.results]]
error = "Found changed value for field `posts`, with record `author:a`, but field is readonly"

[[test.results]]
value = "[{ id: author:a, name: 'c', posts: [post:1] }]"

[[test.results]]
error = "Found changed value for field `posts`, with record `author:a`, but field is readonly"

[[test.results]]
value = "[{ id: author:a, name: 'd', posts: [post:1] }]"

[[test.results]]
error = "Found changed value for field `posts`, with record `author:a`, but field is readonly"

[[test.results]]
value = "[{ id: author:a, name: 'd', posts: [post:1] }]"
*/

DEFINE TABLE author SCHEMAFULL;
DEFINE FIELD name ON author TYPE string;
DEFINE FIELD posts ON author TYPE array<record<post>> DEFAULT [] READONLY;
DEFINE TABLE post SCHEMAFULL;
DEFINE FIELD author ON post TYPE record<author> REFERENCE MATERIALIZE INTO author.posts;
CREATE author:a SET name = 'a';
CREATE post:1 SET author = author:a;
UPDATE author:a SET posts = [];
UPDATE author:a SET posts += post:9;
UPDATE author:a MERGE { posts: [post:9] };
UPDATE author:a CONTENT { name: 'b', posts: [post:9] };
UPDATE author:a CONTENT { name: 'b' };
UPSERT author:a SET posts = [post:9];
UPDATE author:a SET name = 'c';
UPDATE author:a UNSET posts;
UPDATE author:a REPLACE { name: 'd' };
UPDATE author:a REPLACE { name: 'e', posts: [post:9] };
SELECT * FROM author:a;
