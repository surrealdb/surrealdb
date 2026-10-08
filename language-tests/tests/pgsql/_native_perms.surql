/**
[test]
run = false
*/

DEFINE TABLE users SCHEMALESS;
CREATE users:alice SET name = 'alice' RETURN NONE;
CREATE users:bob SET name = 'bob' RETURN NONE;

DEFINE ACCESS test ON DATABASE TYPE RECORD
	SIGNIN ( SELECT * FROM type::record('users', $id) )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE emp SCHEMALESS PERMISSIONS FOR select WHERE city = 'London' OR dept = 1;
DEFINE FIELD salary ON emp PERMISSIONS FOR select WHERE $auth.id = users:alice AND dept != 2;
DEFINE TABLE dept SCHEMALESS PERMISSIONS FOR select WHERE floor < 4;
DEFINE FIELD label ON dept COMPUTED string::uppercase(dname);
DEFINE TABLE proj SCHEMALESS PERMISSIONS NONE;
DEFINE TABLE open SCHEMALESS PERMISSIONS FULL;

CREATE emp:1 SET name = 'Alice', dept = 1, salary = 100, city = 'London' RETURN NONE;
CREATE emp:2 SET name = 'Bob', dept = 1, salary = 80, city = 'Paris' RETURN NONE;
CREATE emp:3 SET name = 'Cara', dept = 2, salary = 120, city = 'London' RETURN NONE;
CREATE emp:4 SET name = 'Dan', dept = 2, salary = 90, city = 'Berlin' RETURN NONE;
CREATE emp:5 SET name = 'Eve', dept = 3, salary = 70, city = 'London' RETURN NONE;
CREATE emp:6 SET name = 'Finn', dept = 3, salary = 60, city = 'Paris' RETURN NONE;

CREATE dept:1 SET did = 1, dname = 'Eng', floor = 3 RETURN NONE;
CREATE dept:2 SET did = 2, dname = 'Ops', floor = 1 RETURN NONE;
CREATE dept:3 SET did = 3, dname = 'Legal', floor = 5 RETURN NONE;

CREATE proj:1 SET emp_id = 1, hours = 10 RETURN NONE;
CREATE proj:2 SET emp_id = 3, hours = 5 RETURN NONE;

CREATE open:1 SET k = 1, v = 'one' RETURN NONE;
CREATE open:2 SET k = 2, v = 'two' RETURN NONE;
CREATE open:3 SET k = 3, v = 'three' RETURN NONE;
