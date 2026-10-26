/**
[env]
namespace = true
database = true

[test]
reason = "Text and casts written to a vector column are the vector they spell, whether the statement inserts or updates, and a distance between vectors of other dimensions is an error"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ embedding: [1f, 2f, 3f], id: 3 }]"

[[test.results]]
value = "[{ embedding: [3f, 2f, 1f], id: 3 }]"

[[test.results]]
value = "[{ embedding: [1f, 2f, 3f], id: 4 }]"

[[test.results]]
error = "Incorrect arguments for function vector::distance::euclidean(). The two vectors must be of the same dimension."

[[test.results]]
value = "[{ embedding: [3f, 2f, 1f], id: 3 }, { embedding: [1f, 2f, 3f], id: 4 }]"
*/

CREATE TABLE pgv_write (id INT PRIMARY KEY, embedding vector(3));

INSERT INTO pgv_write (id, embedding) VALUES (3, '[1,2,3]') RETURNING id, embedding;

UPDATE pgv_write SET embedding = '[3,2,1]' WHERE id = 3 RETURNING id, embedding;

INSERT INTO pgv_write (id, embedding) VALUES (4, '[1,2,3]'::vector(3)) RETURNING id, embedding;

SELECT embedding <-> '[1,2]' AS distance FROM pgv_write;

SELECT id, embedding FROM pgv_write ORDER BY id;
