/**
[env]
namespace = true
database = true

[test]
reason = """
	A component name is a field of the table, so a record id cannot take a name
	that a field already holds. A nested definition under the name (`temp.max`,
	`tags[*]`) counts: names compare by their root. The refused statement
	leaves the table as it was.
"""

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot name `city` in the record id of `reading`: `city` is already a field of `reading`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot name `temp` in the record id of `trip`: `temp` is already a field of `trip`"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot name `tags` in the record id of `post`: `tags` is already a field of `post`"

[[test.results]]
value = "{ events: {  }, fields: { city: 'DEFINE FIELD city ON reading TYPE string PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
*/

-- A field of the same name.
DEFINE FIELD city ON reading TYPE string;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];

-- A field nested under the name.
DEFINE FIELD temp ON trip TYPE object;
DEFINE FIELD temp.max ON trip TYPE int;
DEFINE FIELD id ON trip TYPE [temp: string, time: datetime];

-- The element field of an array.
DEFINE FIELD tags ON post TYPE array<string>;
DEFINE FIELD id ON post TYPE [tags: string, time: datetime];

-- The refusals changed nothing.
INFO FOR TABLE reading;

-- Removing the field frees the name.
REMOVE FIELD city ON reading;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
