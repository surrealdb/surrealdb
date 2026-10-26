/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "An index has one kind, so UNIQUE followed by FULLTEXT is a parse error rather than a full-text index that silently drops the UNIQUE constraint"
issue = 6002

[test.results]
parsing-error = """
Cannot combine FULLTEXT with UNIQUE
  --> [28:71]
   |
28 | ...me UNIQUE FULLTEXT ANALYZER ascii_lcase;
   |              ^^^^^^^^ an index can only be one of UNIQUE, COUNT, FULLTEXT, HNSW, DISKANN or SPATIAL
  --> [28:64]
   |
28 | ... nickname UNIQUE FULLTEXT ANALYZER ascii_lcase;
   |              ^^^^^^ First given here
"""
*/

DEFINE ANALYZER ascii_lcase FILTERS LOWERCASE, ASCII;
DEFINE TABLE users SCHEMAFULL;
DEFINE FIELD nickname ON TABLE users TYPE string;
DEFINE INDEX users_nickname_idx ON TABLE users FIELDS nickname UNIQUE FULLTEXT ANALYZER ascii_lcase;
CREATE users:test1 SET nickname = 'test';
CREATE users:test2 SET nickname = 'test';
