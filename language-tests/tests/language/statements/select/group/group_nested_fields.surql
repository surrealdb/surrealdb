/**
[test]
reason = "Test GROUP BY with nested field paths and objects as group keys"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ address: { city: 'LA' }, avg_age: 31f, count: 2 }, { address: { city: 'London' }, avg_age: 28f, count: 1 }, { address: { city: 'NYC' }, avg_age: 30f, count: 2 }]"

[[test.results]]
value = "[{ address: { city: 'London', country: 'UK' }, count: 1 }, { address: { city: 'LA', country: 'USA' }, count: 2 }, { address: { city: 'NYC', country: 'USA' }, count: 2 }]"

[[test.results]]
value = "[{ address: { city: 'LA', country: 'USA' }, count: 2 }, { address: { city: 'London', country: 'UK' }, count: 1 }, { address: { city: 'NYC', country: 'USA' }, count: 2 }]"

*/

-- Setup data
{
	CREATE user:1 SET address = { city: 'NYC', country: 'USA' }, age = 25;
	CREATE user:2 SET address = { city: 'LA', country: 'USA' }, age = 30;
	CREATE user:3 SET address = { city: 'NYC', country: 'USA' }, age = 35;
	CREATE user:4 SET address = { city: 'London', country: 'UK' }, age = 28;
	CREATE user:5 SET address = { city: 'LA', country: 'USA' }, age = 32;
    RETURN "OK";
};

-- Test grouping by nested field
SELECT address.city, COUNT() as count, math::mean(age) as avg_age FROM user GROUP BY address.city;

-- Test grouping by multiple levels deep
SELECT address.country, address.city, COUNT() as count FROM user GROUP BY address.country, address.city;

-- Test grouping by entire object
SELECT address, COUNT() as count FROM user GROUP BY address;
