/**
[env]
namespace = true
database = true

[test]
reason = "A field is not defined inside a record link, and an element is not defined inside an undefined field"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot define field `owner.name` in field `owner` of type `record<person>`: only an object holds a field"

[[test.results]]
error = "Cannot define field `tags.*`: the field `tags` it is nested in is not defined. Define `tags` first"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'DEFINE FIELD tags.* ON pet TYPE string PERMISSIONS FULL'"
*/

DEFINE TABLE person SCHEMAFULL;
DEFINE TABLE pet SCHEMAFULL;
DEFINE FIELD owner ON pet TYPE record<person>;
DEFINE FIELD owner.name ON pet TYPE string;
DEFINE FIELD tags[*] ON pet TYPE string;
DEFINE FIELD tags ON pet TYPE array<string>;
(INFO FOR TABLE pet).fields.`tags.*`;
