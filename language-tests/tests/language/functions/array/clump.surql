/**
[test]

[[test.results]]
value = "[[0, 1], [2, 3]]"

[[test.results]]
value = "[[0, 1], [2]]"

[[test.results]]
value = "[[0, 1, 2]]"

[[test.results]]
value = "[[0, 1, 2]]"

[[test.results]]
value = "[[0, 1, 2], [3, 4, 5]]"

[[test.results]]
error = "Incorrect arguments for function array::clump(). The second argument must be an integer greater than 0"

[[test.results]]
error = "Incorrect arguments for function array::clump(). The second argument must be an integer greater than 0"

[[test.results]]
value = "[[0], [1], [2]]"

*/
array::clump([0, 1, 2, 3], 2);
array::clump([0, 1, 2], 2);
array::clump([0, 1, 2], 3);
array::clump([0, 1, 2], 4);
array::clump([0, 1, 2, 3, 4, 5], 3);
array::clump([0, 1, 2], 0);
array::clump([0, 1, 2], -1);
array::clump([0, 1, 2], 1);
