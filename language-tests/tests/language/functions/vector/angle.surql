/**
[test]
reason = "Tests the angle between two vectors at exact special points, zero-vector NaN behavior, and dimension mismatch errors"

[[test.results]]
value = "0f"

[[test.results]]
value = "3.141592653589793f"

[[test.results]]
value = "1.5707963267948966f"

[[test.results]]
value = "NaN"

[[test.results]]
value = "NaN"

[[test.results]]
error = "Incorrect arguments for function vector::angle(). The two vectors must be of the same dimension."

[[test.results]]
error = "Incorrect arguments for function vector::angle(). Argument 2 was the wrong type. Expected `array<number>` but found `NULL`"
*/
vector::angle([1, 0], [1, 0]);
vector::angle([1, 0], [-1, 0]);
vector::angle([1, 0], [0, 1]);
vector::angle([0, 0], [1, 1]);
vector::angle([], []);
vector::angle([1, 2], [1, 2, 3]);
vector::angle([1, 2], NULL);
