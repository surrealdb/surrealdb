/**
[env]
namespace = true
database = true

[test]
reason = "A record id component of type `any` is refused: its stored encoding does not keep the order of its values, so a key range could not use it. The error names the table, the component and the type."

[[test.results]]
error = "The record id component `city` of `reading` cannot have the type `any`: only string, int, datetime, uuid, duration, bool and record components keep their order in storage"
*/

DEFINE FIELD id ON reading TYPE [city: any, time: datetime];
