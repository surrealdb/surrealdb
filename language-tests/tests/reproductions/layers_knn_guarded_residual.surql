/**
[env]
imports = ["reproductions/layers_knn_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }
planner-strategy = ["compute-only", "all-ro", "best-effort-ro"]

[test]
reason = """
	A KNN query's residual predicate is judged over the row as the caller may
	see it, not over the stored record the search traverses: a conjunct on a
	guarded field, by name or through `$this`, compares the admitted value on
	the caller's own records and `NONE` on the others. The dataset has no
	more matching rows than the search's `k`, so the engines agree on the
	rows returned.
"""

[[test.results]]
value = "[{ id: docs:1 }]"

[[test.results]]
value = "[{ id: docs:1, secret: 'x' }]"

[[test.results]]
value = "[{ id: docs:1 }]"

[[test.results]]
value = "[{ id: docs:1 }, { id: docs:2 }, { id: docs:3 }]"

[[test.results]]
value = "[{ id: docs:3, secret: 'y' }]"
*/

-- The residual names the guarded field: docs:2 stores 'x' too, but the
-- caller may not read it, so only docs:1 matches.
SELECT id FROM docs WHERE vec <|3, 40|> [0.1f, 0.2f] AND secret = 'x';
SELECT id, secret FROM docs WHERE vec <|3, 40|> [0.1f, 0.2f] AND secret = 'x';
-- The same read through a row parameter.
SELECT id FROM docs WHERE vec <|3, 40|> [0.1f, 0.2f] AND $this.secret = 'x';
-- Without the guarded conjunct every candidate is returned.
SELECT id FROM docs WHERE vec <|3, 40|> [0.1f, 0.2f];
-- The caller's other record.
SELECT id, secret FROM docs WHERE vec <|3, 40|> [0.1f, 0.2f] AND secret = 'y';
