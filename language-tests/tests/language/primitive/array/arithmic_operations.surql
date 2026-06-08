/**
[test]

[[test.results]]
error = "Cannot perform addition with 'array' and 'int'"

[[test.results]]
error = "Cannot perform subtraction with 'array' and 'int'"

[[test.results]]
error = "Cannot perform multiplication with 'array' and 'int'"

[[test.results]]
value = "NaN"

[[test.results]]
error = "Cannot raise the value 'array' with 'int'"

[[test.results]]
error = "Cannot perform remainder with 'array' and 'int'"

[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
error = "Cannot negate the value 'array'"

[[test.results]]
value = "[1, 2, 3, 4]"

*/
[1,2,3] + 1;
[1,2,3] - 1;
[1,2,3] * 1;
[1,2,3] / 1;
[1,2,3] ** 1;
[1,2,3] % 1;

+[1,2,3];
-[1,2,3];

[1,2] + [3,4]
