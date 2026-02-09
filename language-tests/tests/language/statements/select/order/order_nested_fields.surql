/**
[test]
reason = "Test ORDER BY with nested object fields"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:1, user: { address: { city: 'Austin' }, name: 'Alice' } }, { id: item:2, user: { address: { city: 'Boston' }, name: 'Bob' } }, { id: item:3, user: { address: { city: 'Chicago' }, name: 'Charlie' } }]"

[[test.results]]
value = "[{ id: item:3, user: { address: { city: 'Chicago' }, name: 'Charlie' } }, { id: item:2, user: { address: { city: 'Boston' }, name: 'Bob' } }, { id: item:1, user: { address: { city: 'Austin' }, name: 'Alice' } }]"

[[test.results]]
value = "[{ id: item:1, user: { address: { city: 'Austin' }, name: 'Alice' } }, { id: item:2, user: { address: { city: 'Boston' }, name: 'Bob' } }, { id: item:3, user: { address: { city: 'Chicago' }, name: 'Charlie' } }]"
*/

-- Setup: Create records with nested objects
{
	CREATE item:1 SET user = { name: "Alice", address: { city: "Austin" } };
	CREATE item:2 SET user = { name: "Bob", address: { city: "Boston" } };
	CREATE item:3 SET user = { name: "Charlie", address: { city: "Chicago" } };
	RETURN "OK";
};

-- Test ASC: Order by nested field user.address.city
SELECT * FROM item ORDER BY user.address.city ASC;

-- Test DESC
SELECT * FROM item ORDER BY user.address.city DESC;

-- Test: Order by top-level nested field
SELECT * FROM item ORDER BY user.name ASC;
