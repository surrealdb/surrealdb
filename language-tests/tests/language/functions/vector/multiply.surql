/**
[test]
reason = "Tests element-wise vector multiplication, including floats, zero vectors, empty vectors, and dimension mismatch errors"

[[test.results]]
value = "[4, 10, 18]"

[[test.results]]
value = "[3f, 1f]"

[[test.results]]
value = "[0, 0]"

[[test.results]]
value = "[-3, -8]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Incorrect arguments for function vector::multiply(). The two vectors must be of the same dimension."

[[test.results]]
error = "Incorrect arguments for function vector::multiply(). Argument 2 was the wrong type. Expected `array<number>` but found `NULL`"
*/
vector::multiply([1, 2, 3], [4, 5, 6]);
vector::multiply([1.5, 2.0], [2.0, 0.5]);
vector::multiply([1, 2], [0, 0]);
vector::multiply([-1, 2], [3, -4]);
vector::multiply([], []);
vector::multiply([1, 2], [1, 2, 3]);
vector::multiply([1, 2], NULL);
