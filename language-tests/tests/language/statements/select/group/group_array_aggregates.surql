/**
[test]
reason = "Test GROUP BY with array aggregation functions and patterns"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ names: ['apple', 'banana', 'apple', 'banana'], type: 'fruit' }, { names: ['carrot', 'broccoli'], type: 'vegetable' }]"

[[test.results]]
value = "[{ type: 'fruit', unique_names: ['apple', 'banana'] }, { type: 'vegetable', unique_names: ['carrot', 'broccoli'] }]"

[[test.results]]
value = "[{ type: 'fruit', unique_names: ['apple', 'banana'] }, { type: 'vegetable', unique_names: ['carrot', 'broccoli'] }]"

[[test.results]]
value = "[{ count: 4, type: 'fruit' }, { count: 2, type: 'vegetable' }]"

[[test.results]]
value = "[{ all_names: ['apple', 'banana', 'apple', 'banana'], count: 4, type: 'fruit' }, { all_names: ['carrot', 'broccoli'], count: 2, type: 'vegetable' }]"

*/

-- Setup data
{
	CREATE items:1 SET type = 'fruit', name = 'apple';
	CREATE items:2 SET type = 'fruit', name = 'banana';
	CREATE items:3 SET type = 'fruit', name = 'apple';
	CREATE items:4 SET type = 'vegetable', name = 'carrot';
	CREATE items:5 SET type = 'vegetable', name = 'broccoli';
	CREATE items:6 SET type = 'fruit', name = 'banana';
	RETURN "OK";
};

-- Test array::group with GROUP BY
SELECT type, array::group(name) as names FROM items GROUP BY type;

-- Test array::distinct on grouped data
SELECT type, array::distinct(array::group(name)) as unique_names FROM items GROUP BY type;
SELECT type, array::distinct(name) as unique_names FROM items GROUP BY type;

-- Test array::len with grouping
SELECT type, array::len(array::group(name)) as count FROM items GROUP BY type;

-- Test combining array functions with other aggregates
SELECT type, COUNT() as count, array::group(name) as all_names FROM items GROUP BY type;
