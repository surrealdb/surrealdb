/**
[test]
reason = "Test GROUP BY with LIMIT and START for pagination of grouped results"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ total: 25f, type: 'cpu' }, { total: 65f, type: 'disk' }]"

[[test.results]]
value = "[{ total: 65f, type: 'disk' }, { total: 45f, type: 'memory' }]"

[[test.results]]
value = "[{ total: 25f, type: 'cpu' }, { total: 65f, type: 'disk' }, { total: 45f, type: 'memory' }]"

[[test.results]]
value = "[]"

*/

-- Setup data
{
	CREATE metric:1 SET type = 'cpu', value = 10;
	CREATE metric:2 SET type = 'memory', value = 20;
	CREATE metric:3 SET type = 'cpu', value = 15;
	CREATE metric:4 SET type = 'disk', value = 30;
	CREATE metric:5 SET type = 'memory', value = 25;
	CREATE metric:6 SET type = 'network', value = 40;
	CREATE metric:7 SET type = 'disk', value = 35;
	RETURN "OK";
};

-- Test LIMIT on grouped results
SELECT type, math::sum(value) as total FROM metric GROUP BY type LIMIT 2;

-- Test START and LIMIT together
SELECT type, math::sum(value) as total FROM metric GROUP BY type ORDER BY type START 1 LIMIT 2;

-- Test LIMIT with ORDER BY on aggregate
SELECT type, math::sum(value) as total FROM metric GROUP BY type ORDER BY total DESC LIMIT 3;

-- Test START beyond available groups
SELECT type, COUNT() as count FROM metric GROUP BY type START 10;
