/**
[test]
reason = "Setup for 3.00.00_field_overwrite.surql: the old binary writes fields whose types it stores as a `Kind`."
run = false
*/
DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON person TYPE string;
DEFINE FIELD age ON person TYPE int ASSERT $value >= 0;
DEFINE FIELD nick ON person TYPE option<string> DEFAULT 'none';
CREATE person:1 SET name = 'a', age = 3;
