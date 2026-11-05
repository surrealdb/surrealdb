/**
[test]
reason = "Tests element-wise vector subtraction, including floats, empty vectors, dimension mismatch errors, and invalid argument types"

[[test.results]]
value = "[3, 3, 3]"

[[test.results]]
value = "[0, 0]"

[[test.results]]
value = "[0.25f, 0.75f]"

[[test.results]]
value = "[-2, -4]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Incorrect arguments for function vector::subtract(). The two vectors must be of the same dimension."

[[test.results]]
error = "Incorrect arguments for function vector::subtract(). Argument 1 was the wrong type. Expected `array<number>` but found `NONE`"
*/
vector::subtract([4, 5, 6], [1, 2, 3]);
vector::subtract([1, 2], [1, 2]);
vector::subtract([0.5, 1.5], [0.25, 0.75]);
vector::subtract([-1, -2], [1, 2]);
vector::subtract([], []);
vector::subtract([1, 2], [1]);
vector::subtract(NONE, NONE);
