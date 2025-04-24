/**
[test]

[[test.results]]
value = "[3]"

[[test.results]]
error = "Incorrect arguments for function array::push(). Argument 1 was the wrong type. Expected `array` but found `3`"

[[test.results]]
value = "[1, 2, [2, 3]]"

[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
value = "[[1, 2], 3]"

*/
array::push([], 3);
array::push(3, true);
array::push([1,2], [2,3]);
array::push([1,2], 3);
array::push([[1,2]], 3);
