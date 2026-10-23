/**
[test]
run = false
reason = "Setup only: a table whose field Dept is declared as a link and whose rows also hold a plain integer in the separate field dept, for the link_fields_case test of both SQL dialects."
*/

DEFINE TABLE dept SCHEMALESS;
CREATE dept:1 SET dname = 'Engineering' RETURN NONE;
CREATE dept:2 SET dname = 'Sales' RETURN NONE;

DEFINE TABLE person SCHEMALESS;
DEFINE FIELD Dept ON person TYPE option<record<dept>>;

CREATE person:1 SET name = 'Eve', Dept = dept:1, dept = 1 RETURN NONE;
CREATE person:2 SET name = 'Sam', Dept = dept:2, dept = 2 RETURN NONE;
