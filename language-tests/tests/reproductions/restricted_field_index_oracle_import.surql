/**
[test]

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
value = "[{ content: 'The password is hunter2', id: articles:one }]"

[[test.results]]
value = "[{ content: 'Nothing interesting here', id: articles:two }]"

[[test.results]]
value = "[{ content: 'Also mentions hunter2 password', id: articles:three }]"

[[test.results]]
value = "[{ category: 'a', id: docs:one, secret: 'S3CRET_TOKEN_VALUE' }]"

[[test.results]]
value = "[{ category: 'a', id: docs:two, secret: 'other_value' }]"

[[test.results]]
value = "[{ category: 'b', id: docs:three, secret: 'S3CRET_TOKEN_VALUE' }]"

[[test.results]]
value = "[{ embedding: [1f, 0f], id: vectors:one, label: 'a' }]"

[[test.results]]
value = "[{ embedding: [9f, 9f], id: vectors:two, label: 'b' }]"

[[test.results]]
value = "[{ embedding: [1f, 1f], id: vectors:three, label: 'a' }]"
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
    SIGNUP ( CREATE type::record('account', $id) )
    SIGNIN ( SELECT * FROM type::record('account', $id) )
    DURATION FOR TOKEN 1h, FOR SESSION 1h;

-- Full-text: the indexed column is unreadable, so a MATCHES query must not be
-- answered from the index.
DEFINE TABLE articles SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE FIELD content ON articles TYPE string PERMISSIONS FOR select NONE;
DEFINE ANALYZER simple TOKENIZERS BLANK;
DEFINE INDEX idx_content ON articles FIELDS content FULLTEXT ANALYZER simple;

-- B-tree: the restricted leaf is stripped from the WHERE clause and the
-- readable leaf passes, so the record would survive on index membership.
-- `idx_cat_secret` is the compound shape of the same hazard: a multi-column
-- index is withheld when *any* of its columns is restricted. `idx_category`
-- covers only a readable column and must stay usable, so the guard's scope is
-- exercised rather than assumed.
DEFINE TABLE docs SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE FIELD secret ON docs TYPE string PERMISSIONS FOR select NONE;
DEFINE FIELD category ON docs TYPE string PERMISSIONS FOR select FULL;
DEFINE INDEX idx_secret ON docs FIELDS secret;
DEFINE INDEX idx_cat_secret ON docs FIELDS category, secret;
DEFINE INDEX idx_category ON docs FIELDS category;

-- HNSW: result membership and order report which restricted vectors lie
-- nearest a supplied one.
DEFINE TABLE vectors SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE FIELD embedding ON vectors PERMISSIONS FOR select NONE;
DEFINE FIELD label ON vectors PERMISSIONS FOR select FULL;
DEFINE INDEX idx_embedding ON vectors FIELDS embedding HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32 EFC 50 M 8;

CREATE articles:one SET content = 'The password is hunter2';
CREATE articles:two SET content = 'Nothing interesting here';
CREATE articles:three SET content = 'Also mentions hunter2 password';
CREATE docs:one SET secret = 'S3CRET_TOKEN_VALUE', category = 'a';
CREATE docs:two SET secret = 'other_value', category = 'a';
CREATE docs:three SET secret = 'S3CRET_TOKEN_VALUE', category = 'b';
CREATE vectors:one SET embedding = [1f, 0f], label = 'a';
CREATE vectors:two SET embedding = [9f, 9f], label = 'b';
CREATE vectors:three SET embedding = [1f, 1f], label = 'a';
