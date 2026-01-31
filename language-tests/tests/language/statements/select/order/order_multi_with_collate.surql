/**
[test]
reason = "Test ORDER BY multiple columns with COLLATE modifier"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:1, name: 'Alice', city: 'Boston' }, { id: item:2, name: 'alice', city: 'Austin' }, { id: item:3, name: 'Bob', city: 'Chicago' }, { id: item:4, name: 'bob', city: 'Atlanta' }]"
*/

-- Setup: Create records with mixed case names and cities
{
	CREATE item:1 SET name = "Alice", city = "Boston";
	CREATE item:2 SET name = "alice", city = "Austin";
	CREATE item:3 SET name = "Bob", city = "Chicago";
	CREATE item:4 SET name = "bob", city = "Atlanta";
	RETURN "OK";
};

-- Test: Order by name with COLLATE, then city with COLLATE
SELECT * FROM item ORDER BY name COLLATE ASC, city COLLATE ASC;
