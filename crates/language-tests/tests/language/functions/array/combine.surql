/**
[test]

[[test.results]]
value = "[]"

[[test.results]]
error = "Incorrect arguments for function array::combine(). Argument 1 was the wrong type. Expected `array` but found `3`"

[[test.results]]
value = "[[1, 2], [1, 3], [2, 2], [2, 3]]"

[[test.results]]
value = "[[1, 3], [1, 4], [1, 5], [2, 3], [2, 4], [2, 5]]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[1, 4], [1, 5], [2, 4], [2, 5], [3, 4], [3, 5]]"

*/
array::combine([], []);
array::combine(3, true);
array::combine([1,2], [2,3]);
array::combine([1,2], [3,4,5]);
array::combine([], [1,2,3]);
array::combine([1,2,3], []);
array::combine([1,2,3], [4,5]);
