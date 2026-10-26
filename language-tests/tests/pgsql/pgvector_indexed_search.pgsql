/**
[env]
namespace = true
database = true
imports = ["pgsql/_pgvector_indexed.surql"]

[test]
reason = "Rows are found by the hnsw index that ranks by the distance of the ordering, nearest first, with an offset and a where clause, and a row with no vector is not found; a column with two indexes is searched exactly"

[[test.results]]
value = "[{ id: 1 }, { id: 4 }, { id: 3 }]"

[[test.results]]
value = "[{ id: 4 }, { id: 1 }]"

[[test.results]]
value = "[{ id: 4 }, { id: 3 }]"

[[test.results]]
value = "[{ id: 1 }, { id: 4 }, { id: 3 }, { id: 2 }]"

[[test.results]]
value = "[{ id: 2 }, { id: 1 }, { id: 4 }]"

[[test.results]]
value = "[{ id: 3 }, { id: 2 }, { id: 1 }]"
*/

SELECT id FROM pgv_indexed ORDER BY embedding <-> '[1,2,3]' LIMIT 3;

SELECT id FROM pgv_indexed ORDER BY l2_distance(embedding, '[1,0,0]') LIMIT 2 OFFSET 1;

SELECT id FROM pgv_indexed WHERE label <> 'a' ORDER BY embedding <-> '[1,2,3]' LIMIT 2;

SELECT id FROM pgv_indexed ORDER BY embedding <-> '[1,2,3]' LIMIT 10;

SELECT id FROM pgv_indexed ORDER BY direction <#> '[1,2,3]' LIMIT 3;

SELECT id FROM pgv_indexed ORDER BY direction <=> '[1,0,0]' LIMIT 3;
