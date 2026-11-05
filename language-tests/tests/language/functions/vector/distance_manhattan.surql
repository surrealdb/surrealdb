/**
[test]
reason = "Tests the vector::distance::manhattan function"

[[test.results]]
value = "0"

[[test.results]]
value = "13"

[[test.results]]
value = "6"

[[test.results]]
value = "2f"

[[test.results]]
value = "0"

[[test.results]]
error = "Incorrect arguments for function vector::distance::manhattan(). The two vectors must be of the same dimension."

[[test.results]]
error = "Incorrect arguments for function vector::distance::manhattan(). Argument 2 was the wrong type. Expected `array<number>` but found `'not a vector'`"
*/
vector::distance::manhattan([1, 2, 3], [1, 2, 3]);
vector::distance::manhattan([10, 20, 15, 10, 5], [12, 24, 18, 8, 7]);
vector::distance::manhattan([-1, -2], [1, 2]);
vector::distance::manhattan([0.5, 1.5], [2.0, 1.0]);
vector::distance::manhattan([], []);
vector::distance::manhattan([1, 2], [1, 2, 3]);
vector::distance::manhattan([1, 2], "not a vector");
