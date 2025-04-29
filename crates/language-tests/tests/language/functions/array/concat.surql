/**
[test]

[[test.results]]
error = "Incorrect arguments for function array::concat(). Expected at least one argument"

[[test.results]]
value = "[]"

[[test.results]]
error = "Incorrect arguments for function array::concat(). Argument 1 was the wrong type. Expected `array` but found `3`"

[[test.results]]
error = "Incorrect arguments for function array::concat(). Argument 2 was the wrong type. Expected `array` but found `4`"

[[test.results]]
error = "Incorrect arguments for function array::concat(). Argument 2 was the wrong type. Expected `array` but found `'a'`"

[[test.results]]
value = "[1, 2, 3, 4, 3, 4, 5, 6]"

[[test.results]]
value = "[1, 2, 3, 4, 3, 4, 5, 6, 5, 6, 7, 8, 7, 8, 9, 0]"

*/
array::concat();
array::concat([], []);
array::concat(3, true);
array::concat([3], 4);
array::concat([3], "a");
array::concat([1,2,3,4], [3,4,5,6]);
array::concat([1,2,3,4], [3,4,5,6], [5,6,7,8], [7,8,9,0]);
