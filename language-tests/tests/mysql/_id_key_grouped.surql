/**
[test]
run = false
reason = "Setup only: departments, two of which have no employees, and employees, one of which has no department, for the id_key_grouped_outer_join test of both SQL dialects."
*/

DEFINE TABLE dept SCHEMALESS;
CREATE dept:1 SET name = 'eng' RETURN NONE;
CREATE dept:2 SET name = 'ops' RETURN NONE;
CREATE dept:3 SET name = 'hr' RETURN NONE;
CREATE dept:4 SET name = 'legal' RETURN NONE;

DEFINE TABLE emp SCHEMALESS;
CREATE emp:1 SET name = 'Ann', dept = 1 RETURN NONE;
CREATE emp:2 SET name = 'Bob', dept = 1 RETURN NONE;
CREATE emp:3 SET name = 'Cy', dept = 2 RETURN NONE;
CREATE emp:4 SET name = 'Di' RETURN NONE;
