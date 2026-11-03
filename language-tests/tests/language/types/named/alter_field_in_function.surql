/**
[env]
namespace = true
database = true

[test]
reason = "A type an ALTER FIELD in a function body sets is a type the function names: the function is refused while it is missing, and it holds the type against REMOVE TYPE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: The function 'fn::retype' cannot be defined: the type 'missing' it names does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: Cannot remove the type 'mood': the function 'fn::retype' names it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/

DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD f ON t TYPE string;
DEFINE FUNCTION fn::retype() { ALTER FIELD f ON t TYPE missing; };
DEFINE TYPE mood AS 'sad' | 'ok' | 'happy';
DEFINE FUNCTION fn::retype() { ALTER FIELD f ON t TYPE option<mood>; };
REMOVE TYPE mood;
REMOVE FUNCTION fn::retype;
REMOVE TYPE mood;
