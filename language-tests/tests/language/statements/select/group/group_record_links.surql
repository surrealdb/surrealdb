/**
[test]
reason = "Test GROUP BY with record ID fields and related records"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ avg_salary: 60000f, company: company:1, employee_count: 3 }, { avg_salary: 60000f, company: company:2, employee_count: 2 }]"

[[test.results]]
value = "[{ company: { id: company:1, name: 'TechCorp' }, count: 3 }, { company: { id: company:2, name: 'DataInc' }, count: 2 }]"

[[test.results]]
value = "[{ employee_id: employee:1, salary: [50000] }, { employee_id: employee:2, salary: [60000] }, { employee_id: employee:3, salary: [55000] }, { employee_id: employee:4, salary: [65000] }, { employee_id: employee:5, salary: [70000] }]"

*/

-- Setup data
{
	CREATE company:1 SET name = 'TechCorp';
	CREATE company:2 SET name = 'DataInc';
	CREATE employee:1 SET name = 'Alice', company = company:1, salary = 50000;
	CREATE employee:2 SET name = 'Bob', company = company:1, salary = 60000;
	CREATE employee:3 SET name = 'Charlie', company = company:2, salary = 55000;
	CREATE employee:4 SET name = 'Diana', company = company:2, salary = 65000;
	CREATE employee:5 SET name = 'Eve', company = company:1, salary = 70000;
	RETURN "OK";
};

-- Test grouping by record ID field
SELECT company, COUNT() as employee_count, math::mean(salary) as avg_salary FROM employee GROUP BY company;

-- Test grouping by record ID with FETCH
SELECT company, COUNT() as count FROM employee GROUP BY company FETCH company;

-- Test grouping by id field itself
SELECT id as employee_id, salary FROM employee GROUP BY id;
