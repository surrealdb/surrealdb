/**
[test]
reason = """
	Setup for layers_knn_guarded_residual: an HNSW-indexed table whose
	`secret` field is readable only by the record's owner, so a residual
	predicate naming it compares the admitted value on some candidates and
	`NONE` on the others, and a computed field a residual may name through
	`$this`. Runs as owner.
"""
run = false
*/

DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
CREATE user:reader;
CREATE user:other;

DEFINE TABLE docs SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE FIELD owner ON docs TYPE record<user>;
DEFINE FIELD secret ON docs TYPE string PERMISSIONS FOR select WHERE owner = $auth.id;
DEFINE FIELD dbl ON docs TYPE int COMPUTED n * 2;
DEFINE INDEX docs_vec ON docs FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32;
INSERT INTO docs [
	{ id: docs:1, vec: [0.1f, 0.2f], owner: user:reader, secret: 'x', n: 1 },
	{ id: docs:2, vec: [0.2f, 0.2f], owner: user:other, secret: 'x', n: 2 },
	{ id: docs:3, vec: [0.3f, 0.2f], owner: user:reader, secret: 'y', n: 3 }
];
