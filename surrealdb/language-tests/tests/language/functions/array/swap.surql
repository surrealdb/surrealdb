/**
[test]

[[test.results]]
value = "[1, 3, 2, 4, 5]"

[[test.results]]
value = "[1, 2, 3, 4, 5]"

[[test.results]]
value = "[1, 2, 3, 5, 4]"

[[test.results]]
value = "[2, 1, 3, 4, 5]"

[[test.results]]
error = "Incorrect arguments for function array::swap(). Argument 1 is out of range. Expected a number between -5 and 5"

[[test.results]]
error = "Incorrect arguments for function array::swap(). Argument 2 is out of range. Expected a number between -5 and 5"

[[test.results]]
error = "Incorrect arguments for function array::swap(). Argument 1 is out of range. Expected a number between -5 and 5"

*/
array::swap([1,2,3,4,5], 1, 2);
array::swap([1,2,3,4,5], 1, 1);
array::swap([1,2,3,4,5], -1, -2);
array::swap([1,2,3,4,5], -5, -4);
array::swap([1,2,3,4,5], 8, 1);
array::swap([1,2,3,4,5], 1, -8);
array::swap([1,2,3,4,5], -6, 1);
