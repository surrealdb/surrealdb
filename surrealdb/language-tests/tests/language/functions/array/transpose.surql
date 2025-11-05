/**
[test]

[[test.results]]
value = "[[0, 2], [1, 3]]"

[[test.results]]
value = "[[0, 3], [1, 4], [2, NONE]]"

[[test.results]]
value = "[[0, 2], [1, 3], [NONE, 4]]"

[[test.results]]
value = "[[0, 2, 4], [1, 3, 5]]"

[[test.results]]
value = "[[0, 'oops', NULL], [1, NONE, 'sorry'], [2, NONE, NONE]]"

[[test.results]]
value = "[[1, 1, 1], [NONE, 2, 2], [NONE, NONE, 3]]"

[[test.results]]
value = "[[1, NONE, NONE], [1, 2, NONE], [1, 2, 3]]"

*/
array::transpose([[0, 1], [2, 3]]);
array::transpose([[0, 1, 2], [3, 4]]);
array::transpose([[0, 1], [2, 3, 4]]);
array::transpose([[0, 1], [2, 3], [4, 5]]);
array::transpose([[0, 1, 2], "oops", [null, "sorry"]]);
[[1],[1,2],[1,2,3]].transpose();
[[1],[1,2],[1,2,3]].transpose().transpose();
