/**
[env]
namespace = true
database = true

[test]
reason = "DEFINE FIELD IF NOT EXISTS that meets the field stores nothing, so the types its kind and clauses name are not judged and the field is kept, whether its comment is written or evaluated; a definition that is stored is judged"

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
value = "{ f: 'DEFINE FIELD f ON t TYPE int PERMISSIONS FULL' }"

[[test.results]]
error = "Invalid statement: The field 'g' on the table 't' cannot be defined: the type 'missing' it names does not exist"

[[test.results]]
error = "Invalid statement: The field 'g' on the table 't' cannot be defined: the type 'missing' it names does not exist"

*/
DEFINE FIELD f ON t TYPE int;
DEFINE FIELD IF NOT EXISTS f ON t TYPE missing;
DEFINE FIELD IF NOT EXISTS f ON t TYPE int VALUE <missing>$value;
LET $comment = 'evaluated';
DEFINE FIELD IF NOT EXISTS f ON t TYPE missing COMMENT $comment;
(INFO FOR TABLE t).fields;
DEFINE FIELD IF NOT EXISTS g ON t TYPE missing;
DEFINE FIELD IF NOT EXISTS g ON t TYPE missing COMMENT $comment;
