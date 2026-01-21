/**
[test]
reason = "Test ORDER BY with graph relation traversal"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ city_name: ['Los Angeles'], id: person:bob, name: 'Bob' }, { city_name: ['New York'], id: person:charlie, name: 'Charlie' }, { city_name: ['San Francisco'], id: person:alice, name: 'Alice' }]"

[[test.results]]
value = "[{ city_name: ['San Francisco'], id: person:alice, name: 'Alice' }, { city_name: ['New York'], id: person:charlie, name: 'Charlie' }, { city_name: ['Los Angeles'], id: person:bob, name: 'Bob' }]"

*/

-- Setup: Create graph with persons and cities
{
	CREATE person:alice SET name = "Alice";
	CREATE person:bob SET name = "Bob";
	CREATE person:charlie SET name = "Charlie";
	CREATE city:nyc SET name = "New York";
	CREATE city:sf SET name = "San Francisco";
	CREATE city:la SET name = "Los Angeles";
	RELATE person:alice->lives_in->city:sf;
	RELATE person:bob->lives_in->city:la;
	RELATE person:charlie->lives_in->city:nyc;
	RETURN "OK";
};

-- Test ASC: Order by traversed relation field
SELECT id, name, ->lives_in->city.name AS city_name FROM person ORDER BY city_name ASC;

-- Test DESC
SELECT id, name, ->lives_in->city.name AS city_name FROM person ORDER BY city_name DESC;
