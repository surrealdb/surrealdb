/**
[env]
namespace = true
database = true

[test]
reason = """
	A list holds the ids in ascending order of the ids as values, whatever order
	the records were written in, and that is the order the reverse lookup of the
	references returns them in: integer, string, uuid and array ids are mixed in
	one list.
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
value = "[{ id: author:a, posts: [] }]"

[[test.results]]
value = "[{ author: author:a, id: post:30 }]"

[[test.results]]
value = "[{ author: author:a, id: post:b }]"

[[test.results]]
value = "[{ author: author:a, id: post:2 }]"

[[test.results]]
value = "[{ author: author:a, id: post:u'8f2c8a0e-3c1b-4e0a-9d4c-0a5b9f0c6d11' }]"

[[test.results]]
value = "[{ author: author:a, id: post:[1, 'x'] }]"

[[test.results]]
value = "[{ author: author:a, id: post:a }]"

[[test.results]]
value = "[{ author: author:a, id: post:10 }]"

[[test.results]]
value = "[{ author: author:a, id: post:[1, 'a'] }]"

[[test.results]]
value = "[[post:2, post:10, post:30, post:a, post:b, post:u'8f2c8a0e-3c1b-4e0a-9d4c-0a5b9f0c6d11', post:[1, 'a'], post:[1, 'x']]]"

[[test.results]]
value = "[true]"

[[test.results]]
value = "[true]"

[[test.results]]
value = "[true]"
*/

DEFINE TABLE author SCHEMAFULL;
DEFINE FIELD posts ON author TYPE array<record<post>> DEFAULT [] READONLY;
DEFINE TABLE post SCHEMAFULL;
DEFINE FIELD author ON post TYPE record<author> REFERENCE MATERIALIZE INTO author.posts;
CREATE author:a;
CREATE post:30 SET author = author:a;
CREATE post:`b` SET author = author:a;
CREATE post:2 SET author = author:a;
CREATE post:u'8f2c8a0e-3c1b-4e0a-9d4c-0a5b9f0c6d11' SET author = author:a;
CREATE post:[1, 'x'] SET author = author:a;
CREATE post:`a` SET author = author:a;
CREATE post:10 SET author = author:a;
CREATE post:[1, 'a'] SET author = author:a;
SELECT VALUE posts FROM author:a;
SELECT VALUE posts = array::sort(<~post) FROM author:a;
SELECT VALUE posts = <~post FROM author:a;
SELECT VALUE posts = array::sort(posts) FROM author:a;
