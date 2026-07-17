/**
[test]
reason = "Tests the vector::distance::chebyshev function"

[[test.results]]
value = "0f"

[[test.results]]
value = "5f"

[[test.results]]
value = "4f"

[[test.results]]
value = "1.5f"

[[test.results]]
error = "Incorrect arguments for function vector::distance::chebyshev(). The two vectors must be of the same dimension."

[[test.results]]
error = "Incorrect arguments for function vector::distance::chebyshev(). Argument 2 was the wrong type. Expected `array<number>` but found `'abc'`"

[[test.results]]
error = "Incorrect arguments for function vector::distance::chebyshev(). Argument 2 was the wrong type. Expected `array<number>` but found `NONE`"
*/
vector::distance::chebyshev([1, 2, 3], [1, 2, 3]);
vector::distance::chebyshev([1, 2, 3], [4, 6, 8]);
vector::distance::chebyshev([-1, -2], [1, 2]);
vector::distance::chebyshev([0.5, 1.5], [2.0, 1.0]);
vector::distance::chebyshev([1, 2], [1, 2, 3]);
vector::distance::chebyshev([1, 2], "abc");
vector::distance::chebyshev([1, 2], NONE);
