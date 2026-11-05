/**
[test]
reason = "Setup for 3.00.00_database_compatibility.surql: the old binary defines a SCHEMAFULL table with typed fields and leaves rows in it."
run = false
*/
DEFINE TABLE item SCHEMAFULL;
DEFINE FIELD name ON item TYPE string;
DEFINE FIELD qty ON item TYPE int;
CREATE item:1 SET name = 'a', qty = 1;
CREATE item:2 SET name = 'b', qty = 2;
