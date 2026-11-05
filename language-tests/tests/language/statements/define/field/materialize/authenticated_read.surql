/**
[env]
imports = ["language/statements/define/field/materialize/authenticated_read_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = """
	A list is an ordinary field of the referenced record: a record user reads it
	under the permissions of the referenced table and of the field, with no check
	of the records it lists. The list of a record the user may not read is not
	read, a field the user may not select is left out, and a record the user
	creates joins the list of a record the user may not update, since the
	maintenance of a list is no write of the user's.
"""

[[test.results]]
value = "[[post:1, post:2]]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: author:open, posts: [post:1, post:2], visible: true }]"

[[test.results]]
value = "[post:1]"

[[test.results]]
value = "[{ author: author:open, hidden: author:open, id: post:4, owner: person:bob }]"

[[test.results]]
value = "[[post:1, post:2, post:4]]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[post:1, post:2, post:4]]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[post:1, post:2]]"
*/

SELECT VALUE posts FROM author:open;
SELECT VALUE posts FROM author:closed;
SELECT * FROM author:open;
SELECT VALUE id FROM post ORDER BY id;
CREATE post:4 SET owner = person:bob, author = author:open, hidden = author:open;
SELECT VALUE posts FROM author:open;
UPDATE author:open SET posts = [];
SELECT VALUE posts FROM author:open;
DELETE post:4;
SELECT VALUE posts FROM author:open;
