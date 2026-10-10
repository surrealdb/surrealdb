/**
[test]
introduced = ">=3.3.0"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ category: 'a', id: pts_h:1, point: [1f, 2f, 3f, 4f] }, { category: 'b', id: pts_h:2, point: [4f, 5f, 6f, 7f] }, { category: 'a', id: pts_h:3, point: [8f, 9f, 10f, 11f] }, { category: 'a', id: pts_h:4, point: [50f, 50f, 50f, 50f] }, { category: 'b', id: pts_h:5, point: [2f, 3f, 4f, 5f] }]"

[[test.results]]
value = "[{ category: 'a', id: pts_d:1, point: [1f, 2f, 3f, 4f] }, { category: 'b', id: pts_d:2, point: [4f, 5f, 6f, 7f] }, { category: 'a', id: pts_d:3, point: [8f, 9f, 10f, 11f] }, { category: 'a', id: pts_d:4, point: [50f, 50f, 50f, 50f] }, { category: 'b', id: pts_d:5, point: [2f, 3f, 4f, 5f] }]"

*/
DEFINE FIELD category ON pts_h TYPE string;
DEFINE INDEX cat_h ON pts_h FIELDS category;
DEFINE INDEX hnsw_h ON pts_h FIELDS point HNSW DIMENSION 4 DIST EUCLIDEAN TYPE F32 EFC 500 M 12;
DEFINE FIELD category ON pts_d TYPE string;
DEFINE INDEX cat_d ON pts_d FIELDS category;
DEFINE INDEX dann_d ON pts_d FIELDS point DISKANN DIMENSION 4 DIST EUCLIDEAN TYPE F32;
INSERT INTO pts_h [
	{ id: 1, point: [1f, 2f, 3f, 4f], category: 'a' },
	{ id: 2, point: [4f, 5f, 6f, 7f], category: 'b' },
	{ id: 3, point: [8f, 9f, 10f, 11f], category: 'a' },
	{ id: 4, point: [50f, 50f, 50f, 50f], category: 'a' },
	{ id: 5, point: [2f, 3f, 4f, 5f], category: 'b' }
];
INSERT INTO pts_d [
	{ id: 1, point: [1f, 2f, 3f, 4f], category: 'a' },
	{ id: 2, point: [4f, 5f, 6f, 7f], category: 'b' },
	{ id: 3, point: [8f, 9f, 10f, 11f], category: 'a' },
	{ id: 4, point: [50f, 50f, 50f, 50f], category: 'a' },
	{ id: 5, point: [2f, 3f, 4f, 5f], category: 'b' }
];
