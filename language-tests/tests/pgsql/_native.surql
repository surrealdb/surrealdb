/**
[test]
run = false
*/

DEFINE TABLE emp SCHEMALESS;
DEFINE INDEX emp_dept ON emp FIELDS dept;
DEFINE TABLE dept SCHEMALESS;
DEFINE TABLE proj SCHEMALESS;
DEFINE TABLE tag SCHEMALESS;
DEFINE TABLE empty SCHEMALESS;

CREATE emp:1 SET name = 'Alice', dept = 1, salary = 100, city = 'London', mgr = NONE RETURN NONE;
CREATE emp:2 SET name = 'Bob', dept = 1, salary = 80, city = 'Paris', mgr = 1 RETURN NONE;
CREATE emp:3 SET name = 'Cara', dept = 2, salary = 120.5, city = 'London', mgr = 1 RETURN NONE;
CREATE emp:4 SET name = 'Dan', dept = 2, salary = 90, city = 'Berlin', mgr = 3 RETURN NONE;
CREATE emp:5 SET name = 'Eve', dept = 3, salary = 70, city = 'Paris', mgr = 3 RETURN NONE;
CREATE emp:6 SET name = 'Finn', dept = NULL, salary = 60, city = 'London', mgr = 2 RETURN NONE;
CREATE emp:7 SET name = 'Gus', salary = 50, city = 'Berlin', mgr = 2 RETURN NONE;
CREATE emp:8 SET name = 'Hana', dept = 9, salary = 110, city = 'Rome', mgr = 1 RETURN NONE;

CREATE dept:1 SET did = 1, dname = 'Eng', floor = 3 RETURN NONE;
CREATE dept:2 SET did = 2, dname = 'Ops', floor = 1 RETURN NONE;
CREATE dept:3 SET did = 2, dname = 'Ops2', floor = 2 RETURN NONE;
CREATE dept:4 SET did = 4, dname = 'Legal', floor = 5 RETURN NONE;
CREATE dept:5 SET did = NULL, dname = 'Nowhere', floor = 0 RETURN NONE;
CREATE dept:6 SET dname = 'Keyless', floor = 7 RETURN NONE;

CREATE proj:1 SET pid = 1, emp_id = 1, hours = 10 RETURN NONE;
CREATE proj:2 SET pid = 2, emp_id = 1, hours = 20 RETURN NONE;
CREATE proj:3 SET pid = 3, emp_id = 3, hours = 5 RETURN NONE;
CREATE proj:4 SET pid = 4, emp_id = 5, hours = 40 RETURN NONE;
CREATE proj:5 SET pid = 5, emp_id = 99, hours = 1 RETURN NONE;

CREATE tag:1 SET k = 1, label = 'int one' RETURN NONE;
CREATE tag:2 SET k = 1.0, label = 'float one' RETURN NONE;
CREATE tag:3 SET k = 1dec, label = 'dec one' RETURN NONE;
CREATE tag:4 SET k = '1', label = 'string one' RETURN NONE;
CREATE tag:5 SET k = NULL, label = 'null' RETURN NONE;
CREATE tag:6 SET label = 'none' RETURN NONE;
CREATE tag:7 SET k = 2, label = 'two' RETURN NONE;
