/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A block's schema statement that creates the session's database when it is
absent (`DEFINE FUNCTION`, `DEFINE PARAM`, `DEFINE ANALYZER`) leaves it to the
statements after it: each resolves the namespace and database as the catalog
stands when it runs, inside an `IF` branch or a `FOR` body as well. A statement
that needs the database while it is still absent, or after the block removed
it, raises that the database does not exist.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "3"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

[[test.results]]
value = "NONE"

[[test.results]]
value = "3"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The database 'test' does not exist"

[[test.results]]
error = "The database 'test' does not exist"

[[test.results]]
error = "The database 'test' does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, compatibility: 4, configs: {  }, extensions: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"
*/

REMOVE DATABASE test;
{ DEFINE FUNCTION fn::c() { RETURN 3; }; RETURN fn::c() };
REMOVE DATABASE test;
{ DEFINE PARAM $p VALUE 1; SELECT VALUE 1 FROM [1] };
REMOVE DATABASE test;
{ DEFINE ANALYZER an TOKENIZERS blank; RETURN (SELECT VALUE 1 FROM [1]) };
REMOVE DATABASE test;
{ FOR $i IN [1, 2] { DEFINE PARAM OVERWRITE $p VALUE 1; LET $x = (SELECT VALUE $i FROM [1]); }; RETURN $p };
REMOVE DATABASE test;
{ IF true { DEFINE FUNCTION fn::c() { RETURN 3; } }; RETURN fn::c() };
REMOVE DATABASE test;
{ DEFINE TABLE y; RETURN 2 };
{ CREATE x:1; SELECT VALUE 1 FROM [1] };
{ DEFINE PARAM $p VALUE 3; REMOVE DATABASE test; SELECT VALUE 1 FROM [1] };
DEFINE DATABASE test;
INFO FOR DB;
