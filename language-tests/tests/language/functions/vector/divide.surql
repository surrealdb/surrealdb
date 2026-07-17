/**
[test]
reason = "Tests element-wise vector division, including division by zero producing NaN, empty vectors, and dimension mismatch errors"

[[test.results]]
value = "[2, 2]"

[[test.results]]
value = "[0.5f, 0.5f]"

[[test.results]]
value = "[NaN, 1]"

[[test.results]]
value = "[NaN, NaN]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Incorrect arguments for function vector::divide(). The two vectors must be of the same dimension."

[[test.results]]
error = "Incorrect arguments for function vector::divide(). Argument 2 was the wrong type. Expected `array<number>` but found `NONE`"
*/
vector::divide([4, 6], [2, 3]);
vector::divide([1.0, 4.0], [2.0, 8.0]);
vector::divide([1, 2], [0, 2]);
vector::divide([0, 0], [0, 0]);
vector::divide([], []);
vector::divide([1, 2, 3], [1, 2]);
vector::divide([1, 2], NONE);
