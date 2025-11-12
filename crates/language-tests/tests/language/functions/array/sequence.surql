/**
[test]

[[test.results]]
value = "[]"

[[test.results]]
value = "[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[-20, -19, -18, -17, -16, -15, -14, -13, -12, -11]"

[[test.results]]
error = "Incorrect arguments for function array::sequence(). Output must not exceed 1048576 bytes."

[[test.results]]
value = "[]"

*/
array::sequence(0);
array::sequence(10);
array::sequence(-10);
array::sequence(-20,10);
array::sequence(9223372036854775800);
array::sequence(-10,-10);
