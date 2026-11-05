/**
[env]
namespace = true
database = true

[test]
reason = """
	A field's REFERENCE ON DELETE THEN repair runs with the referencing record
	as `$this`, so a name it reads is read by the field: removing the `id` that
	gives the name, or renaming it, is refused naming the field. A repair that
	reads no component does not block the change.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change the record id of `post`: the field `owner` reads its component `city`"

[[test.results]]
error = "Cannot change the record id of `post`: the field `owner` reads its component `city`"

[[test.results]]
value = "'DEFINE FIELD id ON post TYPE [city: string, n: int] PERMISSIONS FULL'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
*/

DEFINE FIELD id ON post TYPE [city: string, n: int];
DEFINE FIELD owner ON post TYPE option<record<person>> REFERENCE ON DELETE THEN { UPDATE $this SET gone = city };
REMOVE FIELD id ON post;
ALTER FIELD id ON post TYPE [place: string, n: int];
(INFO FOR TABLE post).fields.id;

DEFINE FIELD id ON note TYPE [city: string, n: int];
DEFINE FIELD owner ON note TYPE option<record<person>> REFERENCE ON DELETE THEN { UPDATE $this SET owner = NONE };
REMOVE FIELD id ON note;
(INFO FOR TABLE note).fields.id;
