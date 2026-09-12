/**
[env]
imports = ["reproductions/layers_knn_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = """
	A KNN query's residual predicate reads a computed field as the row
	presents it, by name or through `$this`. The legacy engine judges a KNN
	residual over the stored record, where a computed field is absent, so
	this test runs on the streaming engine alone. The plan pins the residual
	above the layer that derives the field, above the search.
"""

[[test.results]]
value = "[{ dbl: 4, id: docs:2 }, { dbl: 6, id: docs:3 }]"

[[test.results]]
value = "[{ id: docs:2 }, { id: docs:3 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: $this.dbl > 2]
        ProjectLayer [ctx: Db] [columns: dbl = <computed> : int @computed]
            KnnScan [ctx: Db] [index: docs_vec, k: 3, ef: 40, dimension: 2]
''''
*/

SELECT id, dbl FROM docs WHERE vec <|3, 40|> [0.1f, 0.2f] AND dbl > 2;
SELECT id FROM docs WHERE vec <|3, 40|> [0.1f, 0.2f] AND $this.dbl > 2;
EXPLAIN SELECT id FROM docs WHERE vec <|3, 40|> [0.1f, 0.2f] AND $this.dbl > 2;
