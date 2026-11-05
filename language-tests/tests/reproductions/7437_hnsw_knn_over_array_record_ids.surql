/**
[test]
reason = "An HNSW kNN query returns the rows whose record ids are arrays"
issue = 7437

[[test.results]]
value = "[{ embeddings: [1f, 0f], id: hnsw_array_id:[1, 'one'] }]"

[[test.results]]
value = "[{ embeddings: [0.9f, 0.1f], id: hnsw_array_id:[2, 'two'] }]"

[[test.results]]
value = "[{ embeddings: [0.8f, 0.2f], id: hnsw_array_id:[3, 'three'] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[hnsw_array_id:[1, 'one'], hnsw_array_id:[2, 'two'], hnsw_array_id:[3, 'three']]"

[[test.results]]
value = "[{ embeddings: [0.7f, 0.3f], id: hnsw_array_id:[4, 'four'] }]"

[[test.results]]
value = "[hnsw_array_id:[1, 'one'], hnsw_array_id:[2, 'two'], hnsw_array_id:[3, 'three'], hnsw_array_id:[4, 'four']]"
*/

CREATE hnsw_array_id:[1, 'one'] SET embeddings = [1.0f, 0.0f];
CREATE hnsw_array_id:[2, 'two'] SET embeddings = [0.9f, 0.1f];
CREATE hnsw_array_id:[3, 'three'] SET embeddings = [0.8f, 0.2f];
DEFINE INDEX embeddings_hnsw ON hnsw_array_id FIELDS embeddings HNSW DIMENSION 2 DIST COSINE TYPE F32;
SELECT VALUE id FROM hnsw_array_id WHERE embeddings <|3,40|> [1.0f, 0.0f];
CREATE hnsw_array_id:[4, 'four'] SET embeddings = [0.7f, 0.3f];
SELECT VALUE id FROM hnsw_array_id WHERE embeddings <|4,40|> [1.0f, 0.0f];
