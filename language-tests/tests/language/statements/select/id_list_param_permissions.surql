/**
[test]
reason = """
	A record user reading a parameter that holds a list of record ids reads only
	the records the table's select permission admits, in list order, as the
	same list written in FROM does, whether the list fits one read window or
	spans several; UPDATE and DELETE of the parameter write only what the
	update and delete permissions admit.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ double: 4, id: author:2, n: 2 }, { double: 8, id: author:4, n: 4 }, { double: 4, id: author:2, n: 2 }]"

[[test.results]]
value = "[{ double: 4, id: author:2, n: 2 }, { double: 8, id: author:4, n: 4 }, { double: 4, id: author:2, n: 2 }]"

[[test.results]]
value = "[4, 8]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "650"

[[test.results]]
value = "[1200, 1202, 1204]"

[[test.results]]
value = "[4]"

[[test.results]]
value = "[]"

[env]
imports = ["language/statements/select/id_list_param_permissions_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }
*/

LET $ids = [author:3, author:2, author:9999, author:4, author:2, author:7];
SELECT * FROM $ids;
SELECT * FROM [author:3, author:2, author:9999, author:4, author:2, author:7];
SELECT VALUE double FROM $ids LIMIT 2;
LET $big = (SELECT VALUE id FROM author);
RETURN array::len(SELECT VALUE n FROM $big);
SELECT VALUE n FROM $big START 600 LIMIT 3;
UPDATE $ids SET seen = true RETURN VALUE n;
DELETE $ids RETURN BEFORE;
