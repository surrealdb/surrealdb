/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ department: 'Engineering', id: department:['Engineering'], totalAwards: 0 }]"

[[test.results]]
value = "[{ awards: 0, department: 'Engineering', id: employee:EMP001 }, { awards: 0, department: 'Engineering', id: employee:EMP003 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ awards: 1, department: 'Engineering', id: employee:EMP004 }]"

[[test.results]]
value = "[{ awards: 1, department: 'Engineering', id: employee:EMP004 }]"

[[test.results]]
value = "[{ department: 'Engineering', id: department:['Engineering'], totalAwards: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

*/

BEGIN;
	DEFINE TABLE employee SCHEMAFULL;
	DEFINE FIELD department ON employee TYPE string;
	DEFINE FIELD awards ON employee TYPE number;

	INSERT INTO employee [
	  {
		id: "EMP001",
		department: "Engineering",
		awards: 0
	  },
	  {
		id: "EMP003",
		department: "Engineering",
		awards: 0
	  },
	];

	DEFINE TABLE department TYPE NORMAL AS
		SELECT
			department,
			math::sum(awards) AS totalAwards
		FROM employee
		GROUP BY department
		PERMISSIONS FOR select FULL, FOR create, update, delete NONE;

	RETURN "OK";
COMMIT;


SELECT * FROM department;
SELECT * FROM employee;
DELETE employee:EMP001;
DELETE employee:EMP003;
SELECT * FROM department;
SELECT * FROM employee;

CREATE employee:EMP004 CONTENT {
	department: "Engineering",
	awards: 1
};
SELECT * FROM employee;
SELECT * FROM department;

DELETE employee:EMP004;
SELECT * FROM department;
