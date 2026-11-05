/**
[env]
namespace = true
database = true

[test]
reason = """
	Defining the clause over records that already reference each other fills every
	list in the same statement: a record with no referencing records gets an empty
	list, and a reference to a record that was never created lists nothing. DEFINE
	FIELD OVERWRITE and ALTER FIELD fill the lists again from the references,
	replacing the lists whatever they held: the condition they state is the one the
	lists are built with. A record created after the references to it was not
	listed, and the next overwrite lists them. Removing the clause stops the
	lists being kept and leaves them as plain data.
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
value = "[{ id: author:a, posts: [] }, { id: author:b, posts: [] }, { id: author:c, posts: [] }]"

[[test.results]]
value = "[{ archived: false, author: author:a, id: post:3 }]"

[[test.results]]
value = "[{ archived: true, author: author:a, id: post:1 }]"

[[test.results]]
value = "[{ archived: false, author: author:b, id: post:2 }]"

[[test.results]]
value = "[{ archived: false, author: author:missing, id: post:4 }]"

[[test.results]]
value = "[[author:a, []], [author:b, []], [author:c, []]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[author:a, [post:1, post:3]], [author:b, [post:2]], [author:c, []]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[author:a, [post:3]], [author:b, [post:2]], [author:c, []]]"

[[test.results]]
value = "[{ id: author:missing, posts: [] }]"

[[test.results]]
value = "[[]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[post:4]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ archived: false, author: author:c, id: post:5 }]"

[[test.results]]
value = "[[author:a, [post:3]], [author:b, [post:2]], [author:c, []], [author:missing, [post:4]]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[author:a, [post:1, post:3]], [author:b, [post:2]], [author:c, [post:5]], [author:missing, [post:4]]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[author:a, [post:1, post:3]], [author:b, [post:2]], [author:c, [post:5]], [author:missing, [post:4]]]"
*/

DEFINE TABLE author SCHEMAFULL;
DEFINE FIELD posts ON author TYPE array<record<post>> DEFAULT [] READONLY;
DEFINE TABLE post SCHEMAFULL;
DEFINE FIELD archived ON post TYPE bool DEFAULT false;
DEFINE FIELD author ON post TYPE record<author> REFERENCE;
CREATE author:a, author:b, author:c;
CREATE post:3 SET author = author:a;
CREATE post:1 SET author = author:a, archived = true;
CREATE post:2 SET author = author:b;
CREATE post:4 SET author = author:missing;
SELECT VALUE [id, posts] FROM author ORDER BY id;
DEFINE FIELD OVERWRITE author ON post TYPE record<author> REFERENCE MATERIALIZE INTO author.posts;
SELECT VALUE [id, posts] FROM author ORDER BY id;
DEFINE FIELD OVERWRITE author ON post TYPE record<author> REFERENCE MATERIALIZE INTO author.posts WHERE archived != true;
SELECT VALUE [id, posts] FROM author ORDER BY id;
CREATE author:missing;
SELECT VALUE posts FROM author:missing;
DEFINE FIELD OVERWRITE author ON post TYPE record<author> REFERENCE MATERIALIZE INTO author.posts WHERE archived != true;
SELECT VALUE posts FROM author:missing;
DEFINE FIELD OVERWRITE author ON post TYPE record<author> REFERENCE;
CREATE post:5 SET author = author:c;
SELECT VALUE [id, posts] FROM author ORDER BY id;
ALTER FIELD author ON post REFERENCE MATERIALIZE INTO author.posts;
SELECT VALUE [id, posts] FROM author ORDER BY id;
DEFINE FIELD OVERWRITE author ON post TYPE record<author> REFERENCE MATERIALIZE INTO author.posts;
SELECT VALUE [id, posts] FROM author ORDER BY id;
