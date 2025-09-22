/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ avgSalary: 75350.5dec, department: 'Engineering', employeeCount: 2, id: department:['Engineering'], totalAwards: 0 }, { avgSalary: 75000dec, department: 'Marketing', employeeCount: 2, id: department:['Marketing'], totalAwards: 2 }]"

[[test.results]]
value = "[{ awards: 0, department: 'Engineering', id: employee:EMP001, salary: 80000f }]"

[[test.results]]
value = "[{ avgSalary: 77850.250dec, department: 'Engineering', employeeCount: 2, id: department:['Engineering'], totalAwards: 0 }, { avgSalary: 75000dec, department: 'Marketing', employeeCount: 2, id: department:['Marketing'], totalAwards: 2 }]"

*/

BEGIN;
	DEFINE TABLE employee SCHEMAFULL;
	DEFINE FIELD id ON employee TYPE string;
	DEFINE FIELD department ON employee TYPE string;
	DEFINE FIELD salary ON employee TYPE float;
	DEFINE FIELD awards ON employee TYPE number;

	INSERT INTO employee [
	  {
		id: "EMP001",
		department: "Engineering",
		salary: 75000.50,
		awards: 0
	  },
	  {
		id: "EMP002",
		department: "Marketing",
		salary: 65000.00,
		awards:1
	  },
	  {
		id: "EMP003",
		department: "Engineering",
		salary: 75700.50,
		awards: 0
	  },
	   {
		id: "EMP004",
		department: "Marketing",
		salary: 85000.00,
		awards: 1
	  }
	];
	RETURN "OK";
COMMIT;

DEFINE TABLE department TYPE NORMAL AS
		SELECT
			department,
			math::mean(salary) AS avgSalary,
			math::sum(awards) AS totalAwards,
			count() AS employeeCount
		FROM employee
		GROUP BY department;

SELECT * FROM department;

UPDATE employee SET salary = 80000.00 WHERE id = employee:EMP001;

SELECT * FROM department;
