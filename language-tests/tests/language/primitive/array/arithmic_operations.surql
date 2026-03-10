/**
[test]

[[test.results]]
error = "Cannot perform addition with '[1, 2, 3]' and '1'"

[[test.results]]
error = "Cannot perform subtraction with '[1, 2, 3]' and '1'"

[[test.results]]
error = "Cannot perform multiplication with '[1, 2, 3]' and '1'"

[[test.results]]
value = "NaN"

[[test.results]]
error = "Cannot raise the value '[1, 2, 3]' with '1'"

[[test.results]]
error = "Cannot perform remainder with '[1, 2, 3]' and '1'"

[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
error = "Cannot negate the value '[1, 2, 3]'"

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