/**
[test]
reason = """
	A FETCH over a table whose select permission is conditional reads the
	same rows, in the same order, as the permission admits them: the rows it
	hides take no place in the paging before the FETCH, and a linked record
	the permission of its own table hides reads as NONE.
"""

[[test.results]]
value = "[{ author: NONE, id: post:1, shown: true }, { author: { id: author:2, n: 2 }, id: post:2, shown: true }, { author: { id: author:4, n: 4 }, id: post:4, shown: true }, { author: NONE, id: post:5, shown: true }, { author: NONE, id: post:7, shown: true }, { author: { id: author:8, n: 8 }, id: post:8, shown: true }, { author: { id: author:10, n: 10 }, id: post:10, shown: true }, { author: NONE, id: post:11, shown: true }, { author: NONE, id: post:13, shown: true }, { author: { id: author:14, n: 14 }, id: post:14, shown: true }, { author: { id: author:16, n: 16 }, id: post:16, shown: true }, { author: NONE, id: post:17, shown: true }, { author: NONE, id: post:19, shown: true }, { author: { id: author:20, n: 20 }, id: post:20, shown: true }, { author: { id: author:22, n: 22 }, id: post:22, shown: true }, { author: NONE, id: post:23, shown: true }, { author: NONE, id: post:25, shown: true }, { author: { id: author:26, n: 26 }, id: post:26, shown: true }, { author: { id: author:28, n: 28 }, id: post:28, shown: true }, { author: NONE, id: post:29, shown: true }, { author: NONE, id: post:31, shown: true }, { author: { id: author:32, n: 32 }, id: post:32, shown: true }, { author: { id: author:34, n: 34 }, id: post:34, shown: true }, { author: NONE, id: post:35, shown: true }, { author: NONE, id: post:37, shown: true }, { author: { id: author:38, n: 38 }, id: post:38, shown: true }]"

[[test.results]]
value = "[{ author: { id: author:4, n: 4 }, id: post:4, shown: true }, { author: NONE, id: post:5, shown: true }, { author: NONE, id: post:7, shown: true }]"

[[test.results]]
value = "[{ author: NONE, id: post:31 }, { author: { id: author:32, n: 32 }, id: post:32 }, { author: { id: author:34, n: 34 }, id: post:34 }, { author: NONE, id: post:35 }, { author: NONE, id: post:37 }, { author: { id: author:38, n: 38 }, id: post:38 }]"

[[test.results]]
value = "[{ author: NONE, id: post:1, shown: true }, { author: { id: author:2, n: 2 }, id: post:2, shown: true }, { author: { id: author:4, n: 4 }, id: post:4, shown: true }, { author: NONE, id: post:5, shown: true }]"

[[test.results]]
value = "[{ author: NONE, id: post:5, shown: true }, { author: NONE, id: post:7, shown: true }, { author: { id: author:8, n: 8 }, id: post:8, shown: true }, { author: { id: author:10, n: 10 }, id: post:10, shown: true }, { author: NONE, id: post:11, shown: true }]"

[env]
imports = ["language/statements/select/fetch/rows_ahead_permissions_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }
*/

SELECT * FROM post FETCH author;
SELECT * FROM post LIMIT 3 START 2 FETCH author;
SELECT id, author FROM post START 20 FETCH author;
SELECT * FROM post LIMIT 4 FETCH author, author;
SELECT * FROM post:5..12 FETCH author;
