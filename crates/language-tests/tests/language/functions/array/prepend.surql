/**
[test]

[[test.results]]
value = "[3]"

[[test.results]]
error = "Incorrect arguments for function array::prepend(). Argument 1 was the wrong type. Expected `array` but found `3`"

[[test.results]]
value = "[[2, 3], 1, 2]"

*/
array::prepend([], 3);
array::prepend(3, true);
array::prepend([1,2], [2,3]);
