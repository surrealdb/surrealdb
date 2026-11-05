/**
[env]
namespace = true
database = true

[test]
reason = "An ALTER that sets a clause or a permission naming a type the database does not define is refused, as DEFINE refuses it, and leaves the definition as it was; an ALTER IF EXISTS of a missing definition judges nothing, and a clause naming a defined type is stored"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: The table 'p' cannot be defined: the type 'missing' it names does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: The event 'e' cannot be defined: the type 'missing' it names does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: The function 'fn::f' cannot be defined: the type 'missing' it names does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: The parameter '$q' cannot be defined: the type 'missing' it names does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: The bucket 'b' cannot be defined: the type 'missing' it names does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: The access method 'acc' cannot be defined: the type 'missing' it names does not exist"

[[test.results]]
error = "Invalid statement: The API config cannot be defined: the type 'missing' it names does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: The API '/a' cannot be defined: the type 'missing' it names does not exist"

[[test.results]]
error = "Invalid statement: The API '/a' cannot be defined: the type 'missing' it names does not exist"

[[test.results]]
error = "Invalid statement: The API '/a' cannot be defined: the type 'missing' it names does not exist"

[[test.results]]
value = "{ p: 'DEFINE TABLE p TYPE ANY SCHEMALESS PERMISSIONS NONE' }"

[[test.results]]
value = "{ e: 'DEFINE EVENT e ON p WHEN true THEN {  }' }"

[[test.results]]
value = "{ f: 'DEFINE FUNCTION fn::f() { RETURN 1 } PERMISSIONS FULL' }"

[[test.results]]
value = "{ q: 'DEFINE PARAM $q VALUE 1 PERMISSIONS FULL' }"

[[test.results]]
value = """{ b: "DEFINE BUCKET b BACKEND 'memory' PERMISSIONS FULL" }"""

[[test.results]]
value = """{ "/a": "DEFINE API '/a' FOR any PERMISSIONS FULL FOR get PERMISSIONS FULL THEN { RETURN { status: 200 } }" }"""

[[test.results]]
value = "{  }"

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

*/
DEFINE TABLE p;
ALTER TABLE p PERMISSIONS FOR select WHERE <missing>'x' = 'x';
DEFINE EVENT e ON p THEN {};
ALTER EVENT e ON p WHEN <missing>$after.status = 'ok';
DEFINE FUNCTION fn::f() { RETURN 1; };
ALTER FUNCTION fn::f() { RETURN <missing>1; };
DEFINE PARAM $q VALUE 1;
ALTER PARAM $q PERMISSIONS WHERE <missing>'x' = 'x';
DEFINE BUCKET b BACKEND "memory";
ALTER BUCKET b PERMISSIONS WHERE <missing>'x' = 'x';
DEFINE ACCESS acc ON DATABASE TYPE RECORD;
ALTER ACCESS acc ON DATABASE AUTHENTICATE { <missing>'x'; $auth };
ALTER CONFIG API PERMISSIONS WHERE <missing>'x' = 'x';
DEFINE API "/a" FOR get THEN { RETURN { status: 200 } };
ALTER API "/a" FOR get THEN { RETURN { status: 200, body: <missing>'x' } };
ALTER API "/a" FOR any THEN { RETURN <missing>'x' };
ALTER API "/a" FOR get PERMISSIONS WHERE <missing>'x' = 'x' THEN { RETURN { status: 200 } };
(INFO FOR DB).tables;
(INFO FOR TABLE p).events;
(INFO FOR DB).functions;
(INFO FOR DB).params;
(INFO FOR DB).buckets;
(INFO FOR DB).apis;
(INFO FOR DB).configs;
ALTER TABLE IF EXISTS nope PERMISSIONS FOR select WHERE <missing>'x' = 'x';
ALTER EVENT IF EXISTS nope ON p WHEN <missing>$after.status = 'ok';
ALTER FUNCTION IF EXISTS fn::nope() { RETURN <missing>1; };
ALTER PARAM IF EXISTS $nope PERMISSIONS WHERE <missing>'x' = 'x';
ALTER BUCKET IF EXISTS nope PERMISSIONS WHERE <missing>'x' = 'x';
ALTER ACCESS IF EXISTS nope ON DATABASE AUTHENTICATE { <missing>'x'; $auth };
ALTER API IF EXISTS "/nope" FOR get THEN { RETURN <missing>'x' };
DEFINE TYPE mood AS 'sad' | 'ok';
ALTER TABLE p PERMISSIONS FOR select WHERE <mood>'ok' = 'ok';
ALTER EVENT e ON p WHEN <mood>'ok' = 'ok';
ALTER FUNCTION fn::f() { RETURN <mood>'ok'; };
ALTER PARAM $q PERMISSIONS WHERE <mood>'ok' = 'ok';
ALTER API "/a" FOR get THEN { RETURN { status: 200, body: <mood>'ok' } };
