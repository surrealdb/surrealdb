/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "Issue #7437: DiskANN pending updates must retain exact numeric variants in complex record IDs"
issue = 7437

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: pts:[1.25f, 'array'], marker: 'array-float', point: [10f] }, { id: pts:[2.50dec, 'array'], marker: 'array-decimal', point: [20f] }, { id: pts:{ kind: 'object', n: 3.25f }, marker: 'object-float', point: [30f] }, { id: pts:{ kind: 'object', n: 4.50dec }, marker: 'object-decimal', point: [40f] }]"

[[test.results]]
match = "string::contains(<string>$result, 'diskann_complex')"

[[test.results]]
value = "[{ marker: 'array-decimal' }, { marker: 'array-float' }, { marker: 'object-decimal' }, { marker: 'object-float' }]"
*/

DEFINE INDEX diskann_complex ON pts FIELDS point DISKANN DIMENSION 1 DIST EUCLIDEAN TYPE F32;
INSERT INTO pts [
	{ id: pts:[1.25f, 'array'], point: [10f], marker: 'array-float' },
	{ id: pts:[2.50dec, 'array'], point: [20f], marker: 'array-decimal' },
	{ id: pts:{ kind: 'object', n: 3.25f }, point: [30f], marker: 'object-float' },
	{ id: pts:{ kind: 'object', n: 4.50dec }, point: [40f], marker: 'object-decimal' }
];
EXPLAIN SELECT marker FROM pts WHERE point <|4,40|> [25f] ORDER BY marker;
SELECT marker FROM pts WHERE point <|4,40|> [25f] ORDER BY marker;
