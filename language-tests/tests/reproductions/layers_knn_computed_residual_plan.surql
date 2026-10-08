/**
[env]
imports = ["reproductions/layers_knn_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }

[test]
reason = """
	Plan companion to layers_knn_computed_residual: a KNN query whose
	residual reads the computed field `dbl`, by name or through `$this`,
	plans the residual filter above the layer that derives `dbl`, which
	stands above the KnnScan; the vector conjunct is consumed by the search
	and does not reappear in the filter.
"""

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id, dbl]
    Filter [ctx: Db] [predicate: dbl > 2]
        ProjectLayer [ctx: Db] [columns: dbl = <computed> : int @computed]
            KnnScan [ctx: Db] [index: docs_vec, k: 3, ef: 40, dimension: 2]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: $this.dbl > 2]
        ProjectLayer [ctx: Db] [columns: dbl = <computed> : int @computed]
            KnnScan [ctx: Db] [index: docs_vec, k: 3, ef: 40, dimension: 2]
''''
*/

EXPLAIN SELECT id, dbl FROM docs WHERE vec <|3, 40|> [0.1f, 0.2f] AND dbl > 2;
EXPLAIN SELECT id FROM docs WHERE vec <|3, 40|> [0.1f, 0.2f] AND $this.dbl > 2;
