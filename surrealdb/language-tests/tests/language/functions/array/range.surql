/**
[test]

[[test.results]]
value = "[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]"

[[test.results]]
value = "[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]"

[[test.results]]
value = "[1, 2, 3, 4, 5, 6, 7, 8, 9, 10,11]"

[[test.results]]
value = "[1, 2, 3, 4, 5, 6, 7, 8, 9]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[-10,-9,-8,-7,-6,-5,-4,-3,-2]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Incorrect arguments for function array::range(). Output must not exceed 1048576 bytes."

*/
array::range(1, 11);
array::range(1..11);
array::range(1..=11);
array::range(1, 10);
array::range(3, 1);
array::range(44, 0);
array::range(0, -1);
array::range(-10, -1);
array::range(0, -256);
array::range(1,9223372036854775800);
