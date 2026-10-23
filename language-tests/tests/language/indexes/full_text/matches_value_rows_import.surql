/**
[test]
reason = """
	Setup for matches_value_rows_*: a full-text indexed table no record user
	may read, holding text the tests probe for, and its twin with no index;
	tables linking to each; an indexed table anyone may read; an indexed table
	each user reads only their own rows of, and its twin with no index; and the
	record user the tests run as. Runs as owner.
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
DEFINE TABLE ndoc SCHEMALESS PERMISSIONS FOR select WHERE false;
CREATE ndoc:1 SET text = 'the quick brown fox';
DEFINE TABLE lnk SCHEMALESS PERMISSIONS FOR select WHERE false;
DEFINE FIELD link ON lnk TYPE record<doc>;
DEFINE TABLE nlnk SCHEMALESS PERMISSIONS FOR select WHERE false;
DEFINE FIELD link ON nlnk TYPE record<ndoc>;
DEFINE TABLE odoc SCHEMALESS PERMISSIONS FOR select WHERE owner = $auth.id;
DEFINE INDEX ft ON odoc FIELDS text FULLTEXT ANALYZER simple BM25;
CREATE odoc:mine SET owner = user:reader, text = 'the quick brown fox';
CREATE odoc:theirs SET owner = user:other, text = 'the quick brown fox';
DEFINE TABLE nodoc SCHEMALESS PERMISSIONS FOR select WHERE owner = $auth.id;
CREATE nodoc:mine SET owner = user:reader, text = 'the quick brown fox';
CREATE nodoc:theirs SET owner = user:other, text = 'the quick brown fox';
DEFINE TABLE fdoc SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE INDEX ft ON fdoc FIELDS text FULLTEXT ANALYZER simple BM25;
