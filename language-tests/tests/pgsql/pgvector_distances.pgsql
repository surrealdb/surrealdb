/**
[env]
namespace = true
database = true

[test]
reason = "pgvector: the distance operators and functions are NULL over a NULL, and compute the distances of pgvector over vectors"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ embedding: [1f, 2f, 3f], id: 1 }, { embedding: NULL, id: 2 }, { embedding: [1f, 0f, 0f], id: 3 }, { embedding: [0f, 1f, 0f], id: 4 }]"

[[test.results]]
value = "[{ id: 1, l2: 5f, l2_function: 5f }]"

[[test.results]]
value = "[{ cosine: 1f, cosine_function: 1f, id: 3 }, { cosine: 0f, cosine_function: 0f, id: 4 }]"

[[test.results]]
value = "[{ inner_product: 32f, l1: 9f, l1_function: 9f, negative_inner_product: -32f }]"

[[test.results]]
value = "[{ cosine: NULL, ip: NULL, l1: NULL, l2: NULL }]"

[[test.results]]
value = "[{ null_operand: NULL }]"

[[test.results]]
value = "[{ spaced: [1f, 2.5f, -30f], v: [1f, 2f, 3f] }]"
*/

CREATE TABLE pgv_values (id INT PRIMARY KEY, embedding vector(3));

INSERT INTO pgv_values (id, embedding) VALUES (1, '[1,2,3]'), (2, NULL), (3, '[1,0,0]'), (4, '[0,1,0]');

SELECT id, embedding <-> '[4,6,3]' AS l2, l2_distance(embedding, '[4,6,3]') AS l2_function FROM pgv_values WHERE id = 1;

SELECT id, embedding <=> '[0,1,0]' AS cosine, cosine_distance(embedding, '[0,1,0]') AS cosine_function FROM pgv_values WHERE id IN (3, 4) ORDER BY id;

SELECT embedding <#> '[4,5,6]' AS negative_inner_product, inner_product(embedding, '[4,5,6]') AS inner_product, embedding <+> '[4,5,6]' AS l1, l1_distance(embedding, '[4,5,6]') AS l1_function FROM pgv_values WHERE id = 1;

SELECT embedding <-> '[1,2,3]' AS l2, embedding <=> '[1,2,3]' AS cosine, embedding <#> '[1,2,3]' AS ip, embedding <+> '[1,2,3]' AS l1 FROM pgv_values WHERE id = 2;

SELECT '[1,2,3]'::vector <-> NULL AS null_operand;

SELECT '[1,2,3]'::vector(3) AS v, '[1, 2.5 , -3e1]'::vector AS spaced;
