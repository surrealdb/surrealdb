/**
[test]

[[test.results]]
value = "[{ id: city:london, points: { type: 'MultiPoint', coordinates: [[-0.118092f, 51.509865f], [-0.118092f, 51.509865f]] } }]"

[[test.results]]
value = "[{ id: city:london, points: { type: 'MultiPoint', coordinates: [[-0.118092f, 51.509865f], [-0.118092f, 51.509865f]] } }]"

[[test.results]]
value = "[{ id: city:london, points: { type: 'MultiPoint', coordinates: [[-0.118092f, 51.509865f], [-0.118092f, 51.509865f]] } }]"

*/
UPSERT city:london SET points = {
	type: 'MultiPoint',
	coordinates: [
		[-0.118092, 51.509865],
		[-0.118092, 51.509865]
	]
};
UPSERT city:london SET points = {
	type: 'MultiPoint',
	coordinates: [
		[-0.118092, 51.509865],
		[-0.118092, 51.509865],
	],
};
SELECT * FROM city:london;
