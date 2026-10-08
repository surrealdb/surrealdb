/**
[test]
reason = "Setup for 3.00.00_field_alter_remove.surql: the old binary writes fields whose types it stores as a `Kind`."
run = false
*/
DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON person TYPE string;
DEFINE FIELD age ON person TYPE option<int>;
DEFINE FIELD tags ON person TYPE array<string>;
CREATE person:1 SET name = 'a', age = 3, tags = ['x'];
