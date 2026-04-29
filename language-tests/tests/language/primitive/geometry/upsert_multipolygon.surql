/**
[test]

[[test.results]]
value = "[{ area: { type: 'MultiPolygon', coordinates: [[[[10f, 11.2f], [10.5f, 11.9f], [10.8f, 12f], [10f, 11.2f]]], [[[9f, 11.2f], [10.5f, 11.9f], [10.3f, 13f], [9f, 11.2f]]]] }, id: university:oxford }]"

[[test.results]]
value = "[{ area: { type: 'MultiPolygon', coordinates: [[[[10f, 11.2f], [10.5f, 11.9f], [10.8f, 12f], [10f, 11.2f]]], [[[9f, 11.2f], [10.5f, 11.9f], [10.3f, 13f], [9f, 11.2f]]]] }, id: university:oxford }]"

[[test.results]]
value = "[{ area: { type: 'MultiPolygon', coordinates: [[[[10f, 11.2f], [10.5f, 11.9f], [10.8f, 12f], [10f, 11.2f]]], [[[9f, 11.2f], [10.5f, 11.9f], [10.3f, 13f], [9f, 11.2f]]]] }, id: university:oxford }]"

*/
UPSERT university:oxford SET area = {
	type: 'MultiPolygon',
	coordinates: [
		[[ [10.0, 11.2], [10.5, 11.9], [10.8, 12.0], [10.0, 11.2] ]],
		[[ [9.0, 11.2], [10.5, 11.9], [10.3, 13.0], [9.0, 11.2] ]]
	]
};
UPSERT university:oxford SET area = {
	type: 'MultiPolygon',
	coordinates: [
		[[ [10.0, 11.2], [10.5, 11.9], [10.8, 12.0], [10.0, 11.2] ]],
		[[ [9.0, 11.2], [10.5, 11.9], [10.3, 13.0], [9.0, 11.2] ]],
	],
};
SELECT * FROM university:oxford;
