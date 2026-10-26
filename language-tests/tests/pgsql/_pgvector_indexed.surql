/**
[test]
run = false
reason = "Setup for pgvector_index_use and pgvector_indexed_search: a table whose vector columns have hnsw indexes, as CREATE INDEX defines them (one over embedding, two over direction), and rows"
*/

DEFINE TABLE pgv_indexed SCHEMAFULL;
DEFINE FIELD embedding ON pgv_indexed TYPE option<array<float, 3>>;
DEFINE FIELD direction ON pgv_indexed TYPE option<array<float, 3>>;
DEFINE FIELD label ON pgv_indexed TYPE option<string>;
DEFINE INDEX pgv_indexed_embedding ON pgv_indexed FIELDS embedding HNSW DIMENSION 3 DIST EUCLIDEAN;
DEFINE INDEX pgv_indexed_direction_cosine ON pgv_indexed FIELDS direction HNSW DIMENSION 3 DIST COSINE;
DEFINE INDEX pgv_indexed_direction_ip ON pgv_indexed FIELDS direction HNSW DIMENSION 3 DIST INNER_PRODUCT;

CREATE pgv_indexed:1 SET embedding = [1, 2, 3], direction = [1, 2, 3], label = 'a' RETURN NONE;
CREATE pgv_indexed:2 SET embedding = [4, 5, 6], direction = [4, 5, 6], label = 'b' RETURN NONE;
CREATE pgv_indexed:3 SET embedding = [1, 0, 0], direction = [1, 0, 0], label = 'c' RETURN NONE;
CREATE pgv_indexed:4 SET embedding = [0, 1, 0], direction = [0, 1, 0], label = 'd' RETURN NONE;
CREATE pgv_indexed:5 SET label = 'e' RETURN NONE;
