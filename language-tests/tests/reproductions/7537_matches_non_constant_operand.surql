/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = """A MATCHES whose query is a row-independent expression — a record path, a field \
or index access on a parameter or a literal — is evaluated once and matched through the \
full-text index, the same as a literal or a LET-bound parameter. A MATCHES conjunct that a \
full-text scan does not serve stays in the residual filter: it is judged per row, or refused \
with the no-index error when its query cannot be resolved, and never dropped."""
issue = 7537

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: user:alice, principals: ['g1', 'g2'] }]"

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ acl: 'g9', e: [1f, 0f], id: doc:1, text: 'hello' }]"

[[test.results]]
value = "[{ acl: 'g2', e: [0.9f, 0.1f], id: doc:2, text: 'hello' }]"

[[test.results]]
value = "[doc:2]"

[[test.results]]
value = "[doc:2]"

[[test.results]]
value = "[doc:2]"

[[test.results]]
value = "[doc:2]"

[[test.results]]
value = "[doc:2]"

[[test.results]]
value = "[doc:2]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[doc:2]"

[[test.results]]
value = "[doc:2]"

[[test.results]]
value = "[doc:2]"

[[test.results]]
value = "[doc:2]"

[[test.results]]
error = "There was no suitable index supporting the expression: acl @2@ (UnwrapExactlyOne)"

[[test.results]]
error = "There was no suitable index supporting the expression: acl @2@ (UnwrapExactlyOne)"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[doc:2]"

[[test.results]]
value = "[doc:2]"
*/

DEFINE TABLE user SCHEMAFULL;
DEFINE FIELD principals ON user TYPE array<string>;
CREATE user:alice SET principals = ['g1', 'g2'];

DEFINE TABLE doc SCHEMAFULL;
DEFINE FIELD acl ON doc TYPE string;
DEFINE FIELD text ON doc TYPE string;
DEFINE FIELD e ON doc TYPE array<float>;
DEFINE ANALYZER simple TOKENIZERS blank;
DEFINE INDEX doc_acl ON doc FIELDS acl FULLTEXT ANALYZER simple;
DEFINE INDEX doc_text ON doc FIELDS text FULLTEXT ANALYZER simple BM25;
DEFINE INDEX doc_e ON doc FIELDS e HNSW DIMENSION 2 DIST COSINE;
CREATE doc:1 SET acl = 'g9', text = 'hello', e = [1.0, 0.0];
CREATE doc:2 SET acl = 'g2', text = 'hello', e = [0.9, 0.1];

-- Literal queries.
SELECT VALUE id FROM doc WHERE text @1@ 'hello' AND acl @2@ 'g2';

-- A scan forced onto one index keeps the other MATCHES in the residual.
SELECT VALUE id FROM doc WITH INDEX doc_text WHERE text @1@ 'hello' AND acl @2@ 'g2';

-- A record path, on its own, next to a KNN operator, and next to another MATCHES.
SELECT VALUE id FROM doc WHERE acl @2@ user:alice.principals[1];
SELECT VALUE id FROM doc WHERE e <|2, 40|> [1.0, 0.0] AND acl @2@ user:alice.principals[1];
SELECT VALUE id FROM doc WHERE text @1@ 'hello' AND acl @2@ user:alice.principals[1];
SELECT VALUE id FROM doc WHERE text @1@ 'hello' AND acl @2,OR@ array::join(user:alice.principals, ' ');

-- Field and index access on a parameter or a literal.
LET $o = { principals: ['g1', 'g2'] };
SELECT VALUE id FROM doc WHERE acl @2@ $o.principals[1];
SELECT VALUE id FROM doc WHERE text @1@ 'hello' AND acl @2@ $o.principals[1];
SELECT VALUE id FROM doc WHERE e <|2, 40|> [1.0, 0.0] AND acl @2@ $o.principals[1];
SELECT VALUE id FROM doc WHERE text @1@ 'hello' AND acl @2@ { a: 'g2' }.a;

-- A subquery is not resolved at plan time: refused, on its own or next to another MATCHES.
SELECT VALUE id FROM doc WHERE acl @2@ (SELECT VALUE principals[1] FROM ONLY user:alice);
SELECT VALUE id FROM doc WHERE text @1@ 'hello' AND acl @2@ (SELECT VALUE principals[1] FROM ONLY user:alice);

-- A LET-bound query.
LET $q = user:alice.principals[1];
SELECT VALUE id FROM doc WHERE text @1@ 'hello' AND acl @2@ $q;
SELECT VALUE id FROM doc WHERE e <|2, 40|> [1.0, 0.0] AND acl @2@ $q;
