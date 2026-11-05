/**
[test]
run = false
reason = "Setup only: tables whose columns are declared as links to one table (required, optional, to another table, to itself) and whose rows hold record links, for the link_fields_* tests of both SQL dialects."
*/

DEFINE TABLE dept SCHEMALESS;
CREATE dept:1 SET dname = 'Engineering' RETURN NONE;
CREATE dept:2 SET dname = 'Sales' RETURN NONE;
CREATE dept:ops SET dname = 'Operations' RETURN NONE;

DEFINE TABLE emp SCHEMAFULL;
DEFINE FIELD name ON emp TYPE string;
DEFINE FIELD dept ON emp TYPE record<dept>;
DEFINE FIELD mentor ON emp TYPE option<record<emp>>;
DEFINE FIELD note ON emp TYPE option<string>;
DEFINE INDEX emp_dept ON emp FIELDS dept;

CREATE emp:1 SET name = 'Eve', dept = dept:1 RETURN NONE;
CREATE emp:2 SET name = 'Sam', dept = dept:2 RETURN NONE;
CREATE emp:3 SET name = 'Ann', dept = dept:1, mentor = emp:1 RETURN NONE;
CREATE emp:4 SET name = 'Bob', dept = dept:ops RETURN NONE;

DEFINE TABLE project SCHEMAFULL;
DEFINE FIELD title ON project TYPE string;
DEFINE FIELD owner ON project TYPE option<record<dept>>;
DEFINE FIELD anyref ON project TYPE option<record>;
DEFINE FIELD either ON project TYPE option<record<dept> | int>;

DEFINE TABLE plain SCHEMALESS;
CREATE plain:1 SET ref = dept:1 RETURN NONE;
