/**
[env]
namespace = true
database = true

[test]
reason = "A type a field expression or another type's ASSERT or DEFAULT names, through a cast, is not removed, and a type is not defined while its ASSERT or DEFAULT names a type that does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: Cannot remove the type 'mood': an expression of the field 'f' on the table 't' names it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: Cannot remove the type 'mood': an expression of the field 'g' on the table 't' names it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: Cannot remove the type 'mood': an expression of the field 'h' on the table 't' names it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: Cannot remove the type 'mood': the type 'wrapper' names it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: Cannot remove the type 'mood': the type 'checked' names it"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: The type 'dangling' cannot be defined: the type 'missing' it names does not exist"

[[test.results]]
value = "NONE"
*/

DEFINE TYPE mood AS 'sad' | 'ok';
DEFINE FIELD f ON t TYPE string DEFAULT <mood> 'ok';
REMOVE TYPE mood;
REMOVE FIELD f ON t;
DEFINE FIELD g ON t TYPE string VALUE <string> <mood> $value;
REMOVE TYPE mood;
REMOVE FIELD g ON t;
DEFINE FIELD h ON t TYPE string ASSERT <mood> $value = 'ok';
REMOVE TYPE mood;
REMOVE FIELD h ON t;
DEFINE TYPE wrapper AS string DEFAULT <mood> 'ok';
REMOVE TYPE mood;
DEFINE TYPE checked AS string ASSERT <mood> $value = 'ok';
REMOVE TYPE wrapper;
REMOVE TYPE mood;
REMOVE TYPE checked;
DEFINE TYPE dangling AS string DEFAULT <missing> 'x';
REMOVE TYPE mood;
