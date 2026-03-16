/**
[test]
reason = "Test ORDER BY with complex nested graph traversal"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ city_population: [900000], id: person:alice, name: 'Alice' }, { city_population: [4000000], id: person:bob, name: 'Bob' }, { city_population: [8000000], id: person:charlie, name: 'Charlie' }]"

[[test.results]]
value = "[{ city_population: [8000000], id: person:charlie, name: 'Charlie' }, { city_population: [4000000], id: person:bob, name: 'Bob' }, { city_population: [900000], id: person:alice, name: 'Alice' }]"

*/

-- Setup: Create graph with persons, companies, and cities
{
	CREATE person:alice SET name = "Alice";
	CREATE person:bob SET name = "Bob";
	CREATE person:charlie SET name = "Charlie";
	CREATE company:acme SET name = "Acme Corp";
	CREATE company:techco SET name = "TechCo";
	CREATE company:bizinc SET name = "BizInc";
	CREATE city:nyc SET name = "New York", population = 8000000;
	CREATE city:sf SET name = "San Francisco", population = 900000;
	CREATE city:la SET name = "Los Angeles", population = 4000000;
	RELATE person:alice->works_at->company:acme;
	RELATE person:bob->works_at->company:techco;
	RELATE person:charlie->works_at->company:bizinc;
	RELATE company:acme->located_in->city:sf;
	RELATE company:techco->located_in->city:la;
	RELATE company:bizinc->located_in->city:nyc;
	RETURN "OK";
};

-- Test ASC: Order by multi-hop graph traversal
SELECT id, name, ->works_at->company->located_in->city.population AS city_population FROM person ORDER BY city_population ASC;

-- Test DESC
SELECT id, name, ->works_at->company->located_in->city.population AS city_population FROM person ORDER BY city_population DESC;
