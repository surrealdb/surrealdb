/**
[env]
namespace = true
database = true

[test]
reason = "RENAME VALUE is refused while a table's record ids hold the union, through a tuple, an array or an alias, since a rename does not rewrite a record id; removing a label is refused while the id field names the type"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "Invalid statement: Cannot rename a value of the type 'mood': the record ids of the table 'r' hold it, and a rename does not rewrite a record id"

[[test.results]]
error = "Invalid statement: Cannot change the values of the type 'mood': the field 'id' on the table 'r' has a type that names it"

[[test.results]]
value = "[r:['sad', 'x']]"

[[test.results]]
value = """{ mood: "DEFINE TYPE mood AS 'sad' | 'ok' PERMISSIONS FULL" }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "Invalid statement: Cannot rename a value of the type 'mood': the record ids of the table 'q' hold it, and a rename does not rewrite a record id"

[[test.results]]
value = "[q:['sad']]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: Cannot rename a value of the type 'mood': the record ids of the table 's' hold it, and a rename does not rewrite a record id"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/
DEFINE TYPE mood AS 'sad' | 'ok';
DEFINE FIELD id ON r TYPE [mood, string];
CREATE r:['sad', 'x'] RETURN NONE;
ALTER TYPE mood RENAME VALUE 'sad' TO 'blue';
DEFINE TYPE OVERWRITE mood AS 'ok';
SELECT VALUE id FROM r;
(INFO FOR DB).types;
REMOVE TABLE r;
DEFINE FIELD id ON q TYPE array<mood>;
CREATE q:['sad'] RETURN NONE;
ALTER TYPE mood RENAME VALUE 'sad' TO 'blue';
SELECT VALUE id FROM q;
REMOVE TABLE q;
DEFINE TYPE feeling AS mood;
DEFINE FIELD id ON s TYPE [feeling, int];
ALTER TYPE mood RENAME VALUE 'sad' TO 'blue';
REMOVE TABLE s;
ALTER TYPE mood RENAME VALUE 'sad' TO 'blue';
