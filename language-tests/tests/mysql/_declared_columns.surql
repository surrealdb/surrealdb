/**
[test]
run = false
reason = "Setup for declared_columns_*: schemafull tables that declare every column they have, and a schemaless table that does not"
*/

DEFINE TABLE declcol_dept SCHEMAFULL;
DEFINE FIELD label ON declcol_dept TYPE string;

DEFINE TABLE declcol_emp SCHEMAFULL;
DEFINE FIELD name ON declcol_emp TYPE string;
DEFINE FIELD dept ON declcol_emp TYPE option<int>;
DEFINE FIELD note ON declcol_emp TYPE option<string>;

DEFINE TABLE declcol_empty SCHEMAFULL;
DEFINE FIELD v ON declcol_empty TYPE int;

DEFINE TABLE declcol_loose SCHEMALESS;

CREATE declcol_dept:1 SET label = 'Eng' RETURN NONE;
CREATE declcol_dept:2 SET label = 'Ops' RETURN NONE;

CREATE declcol_emp:1 SET name = 'Ann', dept = 1, note = 'vip' RETURN NONE;
CREATE declcol_emp:2 SET name = 'Ben', dept = 2 RETURN NONE;
CREATE declcol_emp:3 SET name = 'Cy' RETURN NONE;
CREATE declcol_emp:4 SET name = 'Di', dept = 9 RETURN NONE;

CREATE declcol_loose:1 SET name = 'Ann', extra = 'e' RETURN NONE;
CREATE declcol_loose:2 SET name = 'Ben' RETURN NONE;
