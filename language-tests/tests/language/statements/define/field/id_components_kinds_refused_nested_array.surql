/**
[env]
namespace = true
database = true

[test]
reason = "A record id component of type `array<string>` is refused: its stored encoding does not keep the order of its values, so a key range could not use it. The error names the table, the component and the type."

[[test.results]]
error = "The record id component `tags` of `reading` cannot have the type `array<string>`: only string, int, datetime, uuid, duration, bool and record components keep their order in storage"
*/

DEFINE FIELD id ON reading TYPE [city: string, tags: array<string>];
