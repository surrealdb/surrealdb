/**
[test]

[[test.results]]
value = "[{ address: { city: 'London', id: address:test }, id: person:test }]"

[[test.results]]
value = "[{ address: { city: 'London', id: address:test }, id: person:test }, { city: 'London', id: address:test }]"

*/
CREATE person:test CONTENT {
	address: (CREATE ONLY address CONTENT {
		id: 'test', city: 'London'
	})
};
SELECT * FROM person, address;
