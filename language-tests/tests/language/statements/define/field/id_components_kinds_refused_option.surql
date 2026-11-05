/**
[env]
namespace = true
database = true

[test]
reason = "A record id component of type `option<string>` is refused: an option is a union with NONE, which no record id element can hold, and only the allowed scalar kinds keep their order in storage. The error names the table, the component and the type, rendered as every type is rendered (`option<string>` is `none | string`, as INFO FOR TABLE and coercion errors print it)."

[[test.results]]
error = "The record id component `city` of `reading` cannot have the type `none | string`: only string, int, datetime, uuid, duration, bool and record components keep their order in storage"
*/

DEFINE FIELD id ON reading TYPE [city: option<string>, time: datetime];
