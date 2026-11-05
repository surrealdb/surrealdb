/**
[test]
reason = "Setup for 3.00.00_field_migration.surql: the old binary defines typed fields and leaves rows behind that a later OVERWRITE no longer describes."
run = false
*/
DEFINE TABLE item SCHEMAFULL;
DEFINE FIELD price ON item TYPE string;
DEFINE FIELD qty ON item TYPE int;
CREATE item:1 SET price = '10', qty = 1;
CREATE item:2 SET price = 'n/a', qty = 2;
DEFINE FIELD OVERWRITE price ON item TYPE int | string;
CREATE item:3 SET price = 30, qty = 3;
