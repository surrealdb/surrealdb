/**
[env]
namespace = true
database = true

[test]
reason = "A type that names an unknown type or leads back to itself is refused, and a type is not removed while a field, a function or another type names it"

[[test.results]]
error = "Invalid statement: The type 'a' cannot be defined: the type 'b' it names does not exist"

[[test.results]]
error = "Invalid statement: The type 'self_ref' cannot be defined: its body leads back to itself through the type 'self_ref'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: The type 'b' cannot be defined: its body leads back to itself through the type 'a'"

[[test.results]]
error = "Invalid statement: Cannot remove the type 'b': the type 'a' names it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: Cannot remove the type 'a': the field 'f' on the table 't' has a type that names it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: Cannot remove the type 'a': the function 'fn::g' names it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: The field 'g' on the table 't' cannot be defined: the type 'nope' it names does not exist"

[[test.results]]
error = "Invalid statement: The function 'fn::h' cannot be defined: the type 'nope' it names does not exist"

[[test.results]]
error = "Invalid statement: The function 'fn::k' cannot be defined: the type 'nope' it names does not exist"

[[test.results]]
error = "Invalid statement: The field 'g' on the table 't' cannot be defined: the type 'nope' it names does not exist"

*/

DEFINE TYPE a AS b;
DEFINE TYPE self_ref AS { next: option<self_ref> };
DEFINE TYPE b AS int;
DEFINE TYPE a AS b | string;
DEFINE TYPE OVERWRITE b AS a;
REMOVE TYPE b;
DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD f ON t TYPE option<a>;
REMOVE TYPE a;
REMOVE FIELD f ON t;
DEFINE FUNCTION fn::g($x: a) { RETURN $x };
REMOVE TYPE a;
REMOVE FUNCTION fn::g;
REMOVE TYPE a;
REMOVE TYPE b;
DEFINE FIELD g ON t TYPE nope;
DEFINE FUNCTION fn::h($x: nope) { RETURN $x };
DEFINE FUNCTION fn::k() -> option<nope> { RETURN NONE };
DEFINE FIELD g ON t TYPE array<nope | a>;
