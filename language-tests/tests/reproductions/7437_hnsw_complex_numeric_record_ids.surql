/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "Issue #7437: HNSW pending updates must retain exact numeric variants in complex record IDs"
issue = 7437

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: pts:[1.25f, 'array'], marker: 'array-float', point: [10f] }, { id: pts:[2.50dec, 'array'], marker: 'array-decimal', point: [20f] }, { id: pts:{ kind: 'object', n: 3.25f }, marker: 'object-float', point: [30f] }, { id: pts:{ kind: 'object', n: 4.50dec }, marker: 'object-decimal', point: [40f] }]"

[[test.results]]
match = "string::contains(<string>$result, 'hnsw_complex')"

[[test.results]]
value = "[{ marker: 'array-decimal' }, { marker: 'array-float' }, { marker: 'object-decimal' }, { marker: 'object-float' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ embeddings: [1f, 0f], id: hnsw_array_id:[1, 'one'] }]"

[[test.results]]
value = "[{ embeddings: [0.9f, 0.1f], id: hnsw_array_id:[2, 'two'] }]"

[[test.results]]
value = "[{ embeddings: [0.8f, 0.2f], id: hnsw_array_id:[3, 'three'] }]"

[[test.results]]
value = "NONE"

[[test.results]]
match = "string::contains(<string>$result, 'embeddings_hnsw')"

[[test.results]]
value = "[{ id: hnsw_array_id:[1, 'one'] }, { id: hnsw_array_id:[2, 'two'] }, { id: hnsw_array_id:[3, 'three'] }]"
*/

DEFINE INDEX hnsw_complex ON pts FIELDS point HNSW DIMENSION 1 DIST EUCLIDEAN TYPE F32;
INSERT INTO pts [
	{ id: pts:[1.25f, 'array'], point: [10f], marker: 'array-float' },
	{ id: pts:[2.50dec, 'array'], point: [20f], marker: 'array-decimal' },
	{ id: pts:{ kind: 'object', n: 3.25f }, point: [30f], marker: 'object-float' },
	{ id: pts:{ kind: 'object', n: 4.50dec }, point: [40f], marker: 'object-decimal' }
];
EXPLAIN SELECT marker FROM pts WHERE point <|4,40|> [25f] ORDER BY marker;
SELECT marker FROM pts WHERE point <|4,40|> [25f] ORDER BY marker;

DEFINE TABLE hnsw_array_id SCHEMALESS;
DEFINE FIELD embeddings ON hnsw_array_id TYPE array<float>;

CREATE hnsw_array_id:[1, 'one'] SET embeddings = [1.0f, 0.0f];
CREATE hnsw_array_id:[2, 'two'] SET embeddings = [0.9f, 0.1f];
CREATE hnsw_array_id:[3, 'three'] SET embeddings = [0.8f, 0.2f];

DEFINE INDEX embeddings_hnsw
	ON hnsw_array_id
	FIELDS embeddings
	HNSW DIMENSION 2 DIST COSINE TYPE F32;

EXPLAIN SELECT id
	FROM hnsw_array_id
	WHERE embeddings <|3,40|> [1.0f, 0.0f]
	ORDER BY id;
SELECT id
	FROM hnsw_array_id
	WHERE embeddings <|3,40|> [1.0f, 0.0f]
	ORDER BY id;
