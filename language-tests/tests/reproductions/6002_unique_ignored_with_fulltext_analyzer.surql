/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "DEFINE INDEX combining UNIQUE with FULLTEXT ANALYZER silently drops the UNIQUE constraint instead of rejecting the unsupported combination (issue #6002)"
issue = 6002

[test.results]
parsing-error = """
Cannot combine FULLTEXT with UNIQUE
  --> [27:71]
   |
27 | ...me UNIQUE FULLTEXT ANALYZER ascii_lcase;
   |              ^^^^^^^^ an index can only be one of IDX, UNIQUE, FULLTEXT, COUNT, HNSW, or DISKANN
"""

*/

DEFINE ANALYZER ascii_lcase FILTERS LOWERCASE, ASCII;

DEFINE TABLE users SCHEMAFULL;
DEFINE FIELD nickname ON TABLE users TYPE string;

DEFINE INDEX users_nickname_idx ON TABLE users FIELDS nickname UNIQUE FULLTEXT ANALYZER ascii_lcase;

CREATE users:test1 SET nickname = 'test';
CREATE users:test2 SET nickname = 'test';
