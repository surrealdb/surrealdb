/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A UNIQUE index cannot carry an analyzer: the 2.x SEARCH ANALYZER spelling is a parse error, and a plain UNIQUE index compares the stored value exactly"
issue = 6002

[test.results]
parsing-error = """
Unexpected token `an identifier`, expected Eof
  --> [24:65]
   |
24 | ...me UNIQUE SEARCH ANALYZER ascii_lcase;
   |              ^^^^^^
"""
*/

DEFINE ANALYZER ascii_lcase FILTERS lowercase,ascii;
DEFINE TABLE users SCHEMAFULL;
DEFINE FIELD nickname ON TABLE users TYPE string;
DEFINE INDEX users_nickname_idx ON users FIELDS nickname UNIQUE SEARCH ANALYZER ascii_lcase;
