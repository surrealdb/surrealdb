/**
[env]
imports = ["reproductions/layers_limit_pushdown_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }

[test]
reason = """
	LIMIT and START over a table read through a stack of projection layers
	page the same rows on both engines: a layer sets columns on a row and
	drops none, so the rows the scan counts are the rows the pipeline's
	LIMIT would have counted, whether the user's predicate reads a stored
	field beneath the layers or a computed column above them, and an ORDER
	BY on a computed column sorts every row before the paging. Beneath a
	table permission the paging counts only the rows the permission admits.
"""

[[test.results]]
value = "[{ dbl: 4 }, { dbl: 6 }]"

[[test.results]]
value = "[{ dbl: 6 }, { dbl: 8 }]"

[[test.results]]
value = "[{ dbl: 6 }, { dbl: 8 }]"

[[test.results]]
value = "[{ dbl: 10 }, { dbl: 8 }]"

[[test.results]]
value = "[{ id: gated:2 }]"

[[test.results]]
value = "[{ id: gated:3 }]"
*/

SELECT dbl FROM t LIMIT 2 START 1;
SELECT dbl FROM t WHERE n > 1 LIMIT 2 START 1;
SELECT dbl FROM t WHERE dbl > 2 LIMIT 2 START 1;
SELECT dbl FROM t ORDER BY dbl DESC LIMIT 2;
SELECT id FROM gated LIMIT 1;
SELECT id FROM gated LIMIT 1 START 1;
