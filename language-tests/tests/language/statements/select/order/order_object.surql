/**
[test]
reason = "Test ORDER BY with object field access"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:1, value: { name: 'Alice', age: 25 } }, { id: item:2, value: { name: 'Bob', age: 30 } }, { id: item:3, value: { name: 'Charlie', age: 20 } }]"

[[test.results]]
value = "[{ id: item:3, value: { name: 'Charlie', age: 20 } }, { id: item:2, value: { name: 'Bob', age: 30 } }, { id: item:1, value: { name: 'Alice', age: 25 } }]"

[[test.results]]
value = "[{ id: item:3, value: { name: 'Charlie', age: 20 } }, { id: item:1, value: { name: 'Alice', age: 25 } }, { id: item:2, value: { name: 'Bob', age: 30 } }]"

[[test.results]]
value = "[{ id: item:2, value: { name: 'Bob', age: 30 } }, { id: item:1, value: { name: 'Alice', age: 25 } }, { id: item:3, value: { name: 'Charlie', age: 20 } }]"
*/

-- Setup: Create records with object values
{
	CREATE item:1 SET value = { name: "Alice", age: 25 };
	CREATE item:2 SET value = { name: "Bob", age: 30 };
	CREATE item:3 SET value = { name: "Charlie", age: 20 };
	RETURN "OK";
};

-- Test ASC by name field
SELECT * FROM item ORDER BY value.name ASC;

-- Test DESC by name field
SELECT * FROM item ORDER BY value.name DESC;

-- Test ASC by age field
SELECT * FROM item ORDER BY value.age ASC;

-- Test DESC by age field
SELECT * FROM item ORDER BY value.age DESC;
