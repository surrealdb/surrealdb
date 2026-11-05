/**
[env]
imports = ["bench/util/embeddings-768d-filtered-50000.surql"]

[bench]
reason = "Unfiltered brute-force KNN (`<|10,EUCLIDEAN|>`, a distance neither of the table's ANN indexes serves) over 50k 768-d rows against the catalog parameter `$q`: every record of the table is ranked. The ranking runs in the scan's body over each record's stored bytes and decodes only the 10 it keeps, so this measures reading and scoring the stored vectors; a change that made the ranking decode every record, or read the table on one task where workers have a read view, shows here as a large regression. Dataset: bench/util/embeddings-768d-filtered-50000.surql."
run = true
rebuild = false
warmup = "2s"
sample-size = 30
measurement-time = "20s"

[test]
run = false
*/

SELECT id, vector::distance::knn() AS d FROM record WHERE embedding <|10,EUCLIDEAN|> $q;
