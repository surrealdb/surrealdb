/**
[test]
reason = "Tests element-wise vector addition, including empty vectors, dimension mismatch errors, and invalid argument types"

[[test.results]]
value = "[5, 7, 9]"

[[test.results]]
value = "[2f, 3f]"

[[test.results]]
value = "[0, 0, 0]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Incorrect arguments for function vector::add(). The two vectors must be of the same dimension."

[[test.results]]
error = "Incorrect arguments for function vector::add(). Argument 2 was the wrong type. Expected `array<number>` but found `'abc'`"

[[test.results]]
error = "Incorrect arguments for function vector::add(). Argument 1 was the wrong type. Expected `array<number>` but found `NONE`"

[[test.results]]
error = "Incorrect arguments for function vector::add(). Argument 1 was the wrong type. Expected `number` but found `'👋'` when coercing element at index 0 of `array<number>`"
*/
vector::add([1, 2, 3], [4, 5, 6]);
vector::add([1.5, 2.5], [0.5, 0.5]);
vector::add([-1, -2, -3], [1, 2, 3]);
vector::add([], []);
vector::add([1, 2], [1, 2, 3]);
vector::add([1, 2], "abc");
vector::add(NONE, [1, 2]);
vector::add(["👋", "🌍"], [1, 2]);
