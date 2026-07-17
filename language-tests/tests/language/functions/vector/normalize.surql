/**
[test]
reason = "Tests vector normalization to unit length, including zero-vector NaN behavior, empty vectors, and invalid argument types"

[[test.results]]
value = "[0.6f, 0.8f]"

[[test.results]]
value = "[-0.6f, 0.8f]"

[[test.results]]
value = "[1f]"

[[test.results]]
value = "[NaN, NaN]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[1f, 0f, 0f]"

[[test.results]]
error = "Incorrect arguments for function vector::normalize(). Argument 1 was the wrong type. Expected `array<number>` but found `'not a vector'`"
*/
vector::normalize([3, 4]);
vector::normalize([-3, 4]);
vector::normalize([5]);
vector::normalize([0, 0]);
vector::normalize([]);
vector::normalize([2.0, 0.0, 0.0]);
vector::normalize("not a vector");
