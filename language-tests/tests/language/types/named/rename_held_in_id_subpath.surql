/**
[env]
namespace = true
database = true

[test]
reason = "RENAME VALUE is refused while a field at a path inside a table's id, an object key's field or an array key's element, holds the union, as it is for the id field itself, since a rename does not rewrite a record id; the records keep their keys"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "Invalid statement: Cannot rename a value of the type 'mood': the record ids of the table 'r' hold it, and a rename does not rewrite a record id"

[[test.results]]
value = "[r:{ m: 'sad' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "Invalid statement: Cannot rename a value of the type 'mood': the record ids of the table 's' hold it, and a rename does not rewrite a record id"

[[test.results]]
value = "[s:['sad', 1]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ mood: "DEFINE TYPE mood AS 'blue' | 'ok' PERMISSIONS FULL" }"""

*/
DEFINE TYPE mood AS 'sad' | 'ok';
DEFINE FIELD id ON r TYPE object;
DEFINE FIELD id.m ON r TYPE mood;
CREATE r:{ m: 'sad' } RETURN NONE;
ALTER TYPE mood RENAME VALUE 'sad' TO 'blue';
SELECT VALUE id FROM r;
REMOVE TABLE r;
DEFINE FIELD id ON s TYPE array;
DEFINE FIELD id[0] ON s TYPE mood;
CREATE s:['sad', 1] RETURN NONE;
ALTER TYPE mood RENAME VALUE 'sad' TO 'blue';
SELECT VALUE id FROM s;
REMOVE TABLE s;
ALTER TYPE mood RENAME VALUE 'sad' TO 'blue';
(INFO FOR DB).types;
