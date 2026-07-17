/**
[test]
reason = "Tests vector normalization to unit length, including zero-magnitude errors, empty vectors, and invalid argument types"

[[test.results]]
value = "[0.6f, 0.8f]"

[[test.results]]
value = "[-0.6f, 0.8f]"

[[test.results]]
value = "[1f]"

[[test.results]]
error = "Incorrect arguments for function vector::normalize(). The vector must not have a magnitude of zero."

[[test.results]]
error = "Incorrect arguments for function vector::normalize(). The vector must not have a magnitude of zero."

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
