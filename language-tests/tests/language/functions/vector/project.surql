/**
[test]
reason = "Tests vector projection of one vector onto another, including projection onto a zero vector, empty vectors, and dimension mismatch errors"

[[test.results]]
value = "[1f, 0f, 0f]"

[[test.results]]
value = "[3f, 0f]"

[[test.results]]
value = "[2f, 0f]"

[[test.results]]
value = "[NaN, NaN]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Incorrect arguments for function vector::project(). The two vectors must be of the same dimension."

[[test.results]]
error = "Incorrect arguments for function vector::project(). Argument 1 was the wrong type. Expected `array<number>` but found `NONE`"
*/
vector::project([1, 2, 3], [1, 0, 0]);
vector::project([3, 4], [1, 0]);
vector::project([2, 2], [4, 0]);
vector::project([1, 1], [0, 0]);
vector::project([], []);
vector::project([1, 2], [1, 2, 3]);
vector::project(NONE, [1, 2]);
