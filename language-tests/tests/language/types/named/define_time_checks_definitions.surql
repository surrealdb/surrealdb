/**
[env]
namespace = true
database = true

[test]
reason = "A definition whose stored clauses name an unknown type is refused: an event's WHEN or THEN, a field's permissions or REFERENCE, a function's permission, a table's permissions or view, a parameter's permission, an API, an access method's clauses, an index's COUNT condition, a bucket's permission and the API config's permission"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: The event 'e' cannot be defined: the type 'missing' it names does not exist"

[[test.results]]
error = "Invalid statement: The event 'e' cannot be defined: the type 'missing' it names does not exist"

[[test.results]]
error = "Invalid statement: The field 'f' on the table 't' cannot be defined: the type 'missing' it names does not exist"

[[test.results]]
error = "Invalid statement: The field 'f' on the table 't' cannot be defined: the type 'missing' it names does not exist"

[[test.results]]
error = "Invalid statement: The field 'r' on the table 't' cannot be defined: the type 'missing' it names does not exist"

[[test.results]]
error = "Invalid statement: The function 'fn::f' cannot be defined: the type 'missing' it names does not exist"

[[test.results]]
error = "Invalid statement: The table 'p' cannot be defined: the type 'missing' it names does not exist"

[[test.results]]
error = "Invalid statement: The table 'v' cannot be defined: the type 'missing' it names does not exist"

[[test.results]]
error = "Invalid statement: The parameter '$p' cannot be defined: the type 'missing' it names does not exist"

[[test.results]]
error = "Invalid statement: The API '/feel' cannot be defined: the type 'missing' it names does not exist"

[[test.results]]
error = "Invalid statement: The access method 'a' cannot be defined: the type 'missing' it names does not exist"

[[test.results]]
error = "Invalid statement: The index 'c' cannot be defined: the type 'missing' it names does not exist"

[[test.results]]
error = "Invalid statement: The bucket 'b' cannot be defined: the type 'missing' it names does not exist"

[[test.results]]
error = "Invalid statement: The API config cannot be defined: the type 'missing' it names does not exist"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, compatibility: 4, configs: {  }, extensions: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: { t: 'DEFINE TABLE t TYPE ANY SCHEMALESS PERMISSIONS NONE' }, types: {  }, users: {  } }"

[[test.results]]
value = "{  }"

[[test.results]]
value = "{  }"

[[test.results]]
value = "{  }"

*/
DEFINE TABLE t SCHEMALESS;
DEFINE EVENT e ON t WHEN <missing>$after.status = 'ok' THEN {};
DEFINE EVENT e ON t THEN (CREATE log SET v = <missing>'x');
DEFINE FIELD f ON t TYPE string PERMISSIONS FOR SELECT WHERE <missing>'x' = 'x';
DEFINE FIELD f ON t TYPE string PERMISSIONS FOR create, update WHERE <missing>'x' = 'x';
DEFINE FIELD r ON t TYPE option<record<t>> REFERENCE ON DELETE THEN (UPDATE $this SET v = <missing>'x');
DEFINE FUNCTION fn::f() { RETURN 1 } PERMISSIONS WHERE <missing>'x' = 'x';
DEFINE TABLE p PERMISSIONS FOR select WHERE <missing>'x' = 'x';
DEFINE TABLE v AS SELECT <missing>status AS s FROM t;
DEFINE PARAM $p VALUE 1 PERMISSIONS WHERE <missing>'x' = 'x';
DEFINE API "/feel" FOR get THEN { RETURN { status: 200, body: <missing>'ok' } };
DEFINE ACCESS a ON DATABASE TYPE RECORD SIGNIN (SELECT * FROM t WHERE v = <missing>$v);
DEFINE INDEX c ON t COUNT WHERE <missing>'x' = 'x';
DEFINE BUCKET b BACKEND "memory" PERMISSIONS WHERE <missing>'x' = 'x';
DEFINE CONFIG API PERMISSIONS WHERE <missing>'x' = 'x';
INFO FOR DB;
(INFO FOR TABLE t).events;
(INFO FOR TABLE t).fields;
(INFO FOR TABLE t).indexes;
