/**
[env]
namespace = true
database = true

[test]
reason = "PostgreSQL multi-dimensional arrays: an int[][] column is an array of arrays, a text literal cast to int[][] is parsed as a nested array, and a row round trips through the column"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ grid: [[1i32, 2i32], [3i32, 4i32]], id: 1 }]"

[[test.results]]
value = "[{ grid: [[1i32, 2i32], [3i32, 4i32]], id: 1 }]"

[[test.results]]
value = "[{ g: [[1, 2], [3, 4]] }]"
*/

CREATE TABLE grids (id INT, grid INT[][]);

INSERT INTO grids (id, grid) VALUES (1, '{{1,2},{3,4}}'::int[][]);

SELECT id, grid FROM grids;

SELECT '{{1,2},{3,4}}'::int[][] AS g;
