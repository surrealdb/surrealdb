/**
[test]
reason = """
	Setup for matches_value_rows_*: a full-text indexed table no record user
	may read, holding text the tests probe for, and the record user they run
	as. Runs as owner.
"""
run = false
*/

DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
CREATE user:reader;

DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;
DEFINE TABLE doc SCHEMALESS PERMISSIONS FOR select WHERE false;
DEFINE INDEX ft ON doc FIELDS text FULLTEXT ANALYZER simple BM25;
CREATE doc:1 SET text = 'the quick brown fox';
DEFINE TABLE cdoc SCHEMALESS PERMISSIONS FOR select WHERE false;
DEFINE FIELD c ON cdoc COMPUTED 1;
DEFINE INDEX ft ON cdoc FIELDS text FULLTEXT ANALYZER simple BM25;
CREATE cdoc:1 SET text = 'the quick brown fox';
DEFINE TABLE pdoc SCHEMALESS PERMISSIONS FOR select WHERE true;
DEFINE INDEX ft ON pdoc FIELDS text FULLTEXT ANALYZER simple BM25;
CREATE pdoc:1 SET text = 'the quick brown fox';
