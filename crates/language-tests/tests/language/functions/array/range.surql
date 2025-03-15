/**
[test]

[[test.results]]
value = "[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]"

[[test.results]]
value = "[3]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Incorrect arguments for function array::range(). Argument 1 was the wrong type. Expected a positive number but found -1"

[[test.results]]
error = "Incorrect arguments for function array::range(). Argument 1 was the wrong type. Expected a positive number but found -256"

[[test.results]]
error = "Incorrect arguments for function array::range(). The range overflowed the maximum value for an integer"

*/
array::range(1, 10);
array::range(3, 1);
array::range(44, 0);
array::range(0, -1);
array::range(0, -256);
array::range(9223372036854775800, 100);
