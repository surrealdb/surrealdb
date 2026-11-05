/**
[test]
reason = "Setup for issue 7537: a record user whose principals gate a full-text ACL match."
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( $rid );

DEFINE TABLE user SCHEMAFULL PERMISSIONS FOR select WHERE id = $auth;
DEFINE FIELD principals ON user TYPE array<string>;
CREATE user:alice SET principals = ['g1', 'g2'];

DEFINE TABLE doc SCHEMAFULL PERMISSIONS FOR select FULL;
DEFINE FIELD acl ON doc TYPE string;
DEFINE FIELD text ON doc TYPE string;
DEFINE FIELD e ON doc TYPE array<float>;
DEFINE ANALYZER simple TOKENIZERS blank;
DEFINE INDEX doc_acl ON doc FIELDS acl FULLTEXT ANALYZER simple;
DEFINE INDEX doc_text ON doc FIELDS text FULLTEXT ANALYZER simple BM25;
DEFINE INDEX doc_e ON doc FIELDS e HNSW DIMENSION 2 DIST COSINE;
CREATE doc:1 SET acl = 'g9', text = 'hello', e = [1.0, 0.0];
CREATE doc:2 SET acl = 'g2', text = 'hello', e = [0.9, 0.1];
