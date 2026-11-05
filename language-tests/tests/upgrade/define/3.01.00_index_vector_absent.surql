/**
[env]
imports = ["upgrade/define/3.01.00_index_vector_absent_import.surql"]

[test]
upgrade = true
introduced = "3.1.0"
version = ">=3.4.0-a"
reason = """
ORDER BY vector::distance::<metric>(field, vector) LIMIT k orders every record
of the table, and raises for a record whose vector is NONE. An HNSW or DiskANN
index holds no entry for such a record, so it answers that order only once it
has recorded storing none; an index an older binary built recorded nothing and
keeps the sort. Each ordered read answers what its WITH NOINDEX twin answers:
the rows over a table whose every record holds a vector, and the error over one
with a record that holds none. The plans, and the plan a rebuild restores, are
asserted in the _current sibling.

The upgrade target is bounded to the current checkout, the first whose ORDER BY
accepts a function call and whose planner reads a distance order as a KNN scan."""

[[test.results]]
value = "[vabs_full_h:1, vabs_full_h:3]"

[[test.results]]
value = "[vabs_full_h:1, vabs_full_h:3]"

[[test.results]]
value = "[vabs_full_d:1, vabs_full_d:3]"

[[test.results]]
value = "[vabs_full_d:1, vabs_full_d:3]"

[[test.results]]
error = "Incorrect arguments for function vector::distance::euclidean(). Argument 1 was the wrong type. Expected `array<number>` but found `NONE`"

[[test.results]]
error = "Incorrect arguments for function vector::distance::euclidean(). Argument 1 was the wrong type. Expected `array<number>` but found `NONE`"

[[test.results]]
error = "Incorrect arguments for function vector::distance::euclidean(). Argument 1 was the wrong type. Expected `array<number>` but found `NONE`"

[[test.results]]
error = "Incorrect arguments for function vector::distance::euclidean(). Argument 1 was the wrong type. Expected `array<number>` but found `NONE`"

*/
-- Every record holds a vector: the nearest two, whichever path answers.
SELECT VALUE id FROM vabs_full_h ORDER BY vector::distance::euclidean(point, [0f, 0f]) LIMIT 2;
SELECT VALUE id FROM vabs_full_h WITH NOINDEX ORDER BY vector::distance::euclidean(point, [0f, 0f]) LIMIT 2;
SELECT VALUE id FROM vabs_full_d ORDER BY vector::distance::euclidean(point, [0f, 0f]) LIMIT 2;
SELECT VALUE id FROM vabs_full_d WITH NOINDEX ORDER BY vector::distance::euclidean(point, [0f, 0f]) LIMIT 2;
-- A record without a vector raises in the order, which the index must not hide.
SELECT VALUE id FROM vabs_none_h ORDER BY vector::distance::euclidean(point, [0f, 0f]) LIMIT 2;
SELECT VALUE id FROM vabs_none_h WITH NOINDEX ORDER BY vector::distance::euclidean(point, [0f, 0f]) LIMIT 2;
SELECT VALUE id FROM vabs_none_d ORDER BY vector::distance::euclidean(point, [0f, 0f]) LIMIT 2;
SELECT VALUE id FROM vabs_none_d WITH NOINDEX ORDER BY vector::distance::euclidean(point, [0f, 0f]) LIMIT 2;
