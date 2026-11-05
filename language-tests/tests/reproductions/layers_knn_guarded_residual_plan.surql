/**
[env]
imports = ["reproductions/layers_knn_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }

[test]
reason = """
	Plan companion to layers_knn_guarded_residual: a KNN query whose
	residual reads the guarded field `secret`, by name or through `$this`,
	plans the residual filter above the layer that guards `secret`, which
	stands above the KnnScan; projecting `secret` beside it adds no second
	guard layer. Without a guarded conjunct the search feeds the projection
	directly, with no layer.
"""

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: secret = 'x']
        ProjectLayer [ctx: Db] [columns: secret = <permission> @permission]
            KnnScan [ctx: Db] [index: docs_vec, k: 3, ef: 40, dimension: 2]
"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id, secret]
    Filter [ctx: Db] [predicate: secret = 'x']
        ProjectLayer [ctx: Db] [columns: secret = <permission> @permission]
            KnnScan [ctx: Db] [index: docs_vec, k: 3, ef: 40, dimension: 2]
"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: $this.secret = 'x']
        ProjectLayer [ctx: Db] [columns: secret = <permission> @permission]
            KnnScan [ctx: Db] [index: docs_vec, k: 3, ef: 40, dimension: 2]
"'''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    KnnScan [ctx: Db] [index: docs_vec, k: 3, ef: 40, dimension: 2]
''''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id, secret]
    Filter [ctx: Db] [predicate: secret = 'y']
        ProjectLayer [ctx: Db] [columns: secret = <permission> @permission]
            KnnScan [ctx: Db] [index: docs_vec, k: 3, ef: 40, dimension: 2]
"'''
*/

-- The residual names the guarded field: docs:2 stores 'x' too, but the
-- caller may not read it, so only docs:1 matches.
EXPLAIN SELECT id FROM docs WHERE vec <|3, 40|> [0.1f, 0.2f] AND secret = 'x';
EXPLAIN SELECT id, secret FROM docs WHERE vec <|3, 40|> [0.1f, 0.2f] AND secret = 'x';
-- The same read through a row parameter.
EXPLAIN SELECT id FROM docs WHERE vec <|3, 40|> [0.1f, 0.2f] AND $this.secret = 'x';
-- Without the guarded conjunct every candidate is returned.
EXPLAIN SELECT id FROM docs WHERE vec <|3, 40|> [0.1f, 0.2f];
-- The caller's other record.
EXPLAIN SELECT id, secret FROM docs WHERE vec <|3, 40|> [0.1f, 0.2f] AND secret = 'y';
