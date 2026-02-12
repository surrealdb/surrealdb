/**
[test]
reason = "Test array::group with GROUP BY"

[[test.results]]
value = "'OK'"

[[test.results]]
value = '[{ "array::group": [1, 2], user: 1 }, { "array::group": [1, 2], user: 2 }]'
*/

{
	CREATE test:1 SET user = 1, role = 1;
	CREATE test:2 SET user = 1, role = 2;
	CREATE test:3 SET user = 2, role = 1;
	CREATE test:4 SET user = 2, role = 2;
	RETURN "OK";
};

SELECT user, array::group(role) FROM test GROUP BY user;
