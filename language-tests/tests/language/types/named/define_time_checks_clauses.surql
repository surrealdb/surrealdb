/**
[env]
namespace = true
database = true

[test]
reason = "A field whose DEFAULT, VALUE, ASSERT or COMPUTED clause names an unknown type, a function whose body does, and an ALTER FIELD that sets a type or a clause naming one, are refused"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: The field 'f' on the table 't' cannot be defined: the type 'missing' it names does not exist"

[[test.results]]
error = "Invalid statement: The field 'f' on the table 't' cannot be defined: the type 'missing' it names does not exist"

[[test.results]]
error = "Invalid statement: The field 'f' on the table 't' cannot be defined: the type 'missing' it names does not exist"

[[test.results]]
error = "Invalid statement: The field 'c' on the table 't' cannot be defined: the type 'missing' it names does not exist"

[[test.results]]
error = "Invalid statement: The function 'fn::f' cannot be defined: the type 'missing' it names does not exist"

[[test.results]]
error = "Invalid statement: The function 'fn::g' cannot be defined: the type 'missing' it names does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: The field 'f' on the table 't' cannot be defined: the type 'missing' it names does not exist"

[[test.results]]
error = "Invalid statement: The field 'f' on the table 't' cannot be defined: the type 'missing' it names does not exist"

[[test.results]]
error = "Invalid statement: The field 'f' on the table 't' cannot be defined: the type 'missing' it names does not exist"

[[test.results]]
error = "Invalid statement: The field 'f' on the table 't' cannot be defined: the type 'missing' it names does not exist"

[[test.results]]
error = "Invalid statement: The field 'f' on the table 't' cannot be defined: the type 'missing' it names does not exist"

[[test.results]]
value = "{ f: 'DEFINE FIELD f ON t TYPE string PERMISSIONS FULL' }"

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

*/

DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD f ON t TYPE string DEFAULT <missing> 'x';
DEFINE FIELD f ON t TYPE string VALUE <missing> $value;
DEFINE FIELD f ON t TYPE string ASSERT <missing> $value = 'x';
DEFINE FIELD c ON t COMPUTED <missing> 'x';
DEFINE FUNCTION fn::f() { RETURN <missing> 'x'; };
DEFINE FUNCTION fn::g() { LET $x: missing = 'x'; RETURN $x; };
DEFINE FIELD f ON t TYPE string;
ALTER FIELD f ON t TYPE missing;
ALTER FIELD f ON t TYPE option<missing>;
ALTER FIELD f ON t DEFAULT <missing> 'x';
ALTER FIELD f ON t ASSERT <missing> $value = 'x';
ALTER FIELD f ON t VALUE <missing> $value;
(INFO FOR TABLE t).fields;
(INFO FOR DB).functions;
DEFINE TYPE present AS string;
DEFINE FIELD d ON t TYPE string DEFAULT <present> 'x';
DEFINE FUNCTION fn::h() { RETURN <present> 'x'; };
ALTER FIELD f ON t TYPE present;
