/**
[test]

[[test.results]]
value = "[1, 1, 1, 1, 1, 1, 1, 1, 1, 1]"

[[test.results]]
value = "['hello', 'hello']"

[[test.results]]
value = "[NONE, NONE, NONE]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Incorrect arguments for function array::repeat(). Output must not exceed 1048576 bytes."

[[test.results]]
error = "Incorrect arguments for function array::repeat(). Output must not exceed 1048576 bytes."

*/
array::repeat(1, 10);
array::repeat("hello", 2);
array::repeat(NONE, 3);
array::repeat(44, 0);
array::repeat(0, -256);
array::repeat(0, 9_223_372_036_854_775_807);
