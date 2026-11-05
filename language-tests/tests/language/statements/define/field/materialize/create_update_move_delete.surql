/**
[env]
namespace = true
database = true

[test]
reason = """
	A REFERENCE field with MATERIALIZE INTO keeps, on each referenced record, the
	ids of the records that reference it, in ascending id order. A create joins
	the list, an update of another field leaves it as it is, an update of the
	reference moves the id from one list to the other, and a delete removes it.
	Each list is compared with the reverse lookup of the references.
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
value = "[{ id: author:a, name: 'x', posts: [] }, { id: author:b, name: 'x', posts: [] }]"

[[test.results]]
value = "[{ author: author:a, id: post:3, title: 'c' }]"

[[test.results]]
value = "[{ author: author:a, id: post:1, title: 'a' }]"

[[test.results]]
value = "[{ author: author:b, id: post:2, title: 'b' }]"

[[test.results]]
value = "[[author:a, [post:1, post:3], [post:1, post:3]], [author:b, [post:2], [post:2]]]"

[[test.results]]
value = "[{ author: author:a, id: post:1, title: 'renamed' }]"

[[test.results]]
value = "[[post:1, post:3]]"

[[test.results]]
value = "[{ author: author:b, id: post:1, title: 'renamed' }]"

[[test.results]]
value = "[[author:a, [post:3], [post:3]], [author:b, [post:1, post:2], [post:1, post:2]]]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[author:a, [], []], [author:b, [post:1, post:2], [post:1, post:2]]]"
*/

DEFINE TABLE author SCHEMAFULL;
DEFINE FIELD name ON author TYPE string;
DEFINE FIELD posts ON author TYPE array<record<post>> DEFAULT [] READONLY;
DEFINE TABLE post SCHEMAFULL;
DEFINE FIELD title ON post TYPE string;
DEFINE FIELD author ON post TYPE record<author> REFERENCE ON DELETE CASCADE MATERIALIZE INTO author.posts;
CREATE author:a, author:b SET name = 'x';
CREATE post:3 SET author = author:a, title = 'c';
CREATE post:1 SET author = author:a, title = 'a';
CREATE post:2 SET author = author:b, title = 'b';
SELECT VALUE [id, posts, array::sort(<~post)] FROM author ORDER BY id;
UPDATE post:1 SET title = 'renamed';
SELECT VALUE posts FROM author:a;
UPDATE post:1 SET author = author:b;
SELECT VALUE [id, posts, array::sort(<~post)] FROM author ORDER BY id;
DELETE post:3;
SELECT VALUE [id, posts, array::sort(<~post)] FROM author ORDER BY id;
