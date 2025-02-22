/**
[test]

[[test.results]]
value = "[[0], [1], [2], [3]]"

[[test.results]]
value = "[[0, 1], [1, 2], [2, 3]]"

[[test.results]]
value = "[[0, 1], [1, 2]]"

[[test.results]]
value = "[[0, 1, 2]]"

[[test.results]]
value = "[[0, 1, 2], [1, 2, 3], [2, 3, 4], [3, 4, 5]]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Incorrect arguments for function array::windows(). The second argument must be an integer greater than 0"

*/
array::windows([0, 1, 2, 3], 1);
array::windows([0, 1, 2, 3], 2);
array::windows([0, 1, 2], 2);
array::windows([0, 1, 2], 3);
array::windows([0, 1, 2, 3, 4, 5], 3);
array::windows([0, 1, 2], 4);
array::windows([0, 1, 2], 0);
