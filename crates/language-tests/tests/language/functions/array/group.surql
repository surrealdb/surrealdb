/**
[test]

[[test.results]]
value = "[]"

[[test.results]]
error = "Incorrect arguments for function array::group(). Argument 1 was the wrong type. Expected `array` but found `3`"

[[test.results]]
value = "[1, 2, 3, 4, 5, 6]"

[[test.results]]
value = "[[1], [2], 1, 2]"

*/
array::group([]);
array::group(3);
array::group([ [1,2,3,4], [3,4,5,6] ]);
array::group([ [[1]], [[2]],1,2, [1,2]]);
