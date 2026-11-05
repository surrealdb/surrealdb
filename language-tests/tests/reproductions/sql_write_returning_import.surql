/**
[test]
run = false
*/
DEFINE TABLE write_returning SCHEMALESS;
DEFINE FIELD age ON write_returning TYPE int;
DEFINE FIELD name ON write_returning TYPE string;
