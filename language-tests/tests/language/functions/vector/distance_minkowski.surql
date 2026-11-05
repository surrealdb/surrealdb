/**
[test]
reason = "Tests the vector::distance::minkowski function"

[[test.results]]
value = "0f"

[[test.results]]
value = "13f"

[[test.results]]
value = "7.0710678118654755f"

[[test.results]]
error = "Incorrect arguments for function vector::distance::minkowski(). The two vectors must be of the same dimension."

[[test.results]]
error = "Incorrect arguments for function vector::distance::minkowski(). Expected 3 arguments"

[[test.results]]
error = "Incorrect arguments for function vector::distance::minkowski(). Argument 3 was the wrong type. Expected `number` but found `'two'`"
*/
vector::distance::minkowski([1, 2, 3], [1, 2, 3], 1);
vector::distance::minkowski([10, 20, 15, 10, 5], [12, 24, 18, 8, 7], 1);
vector::distance::minkowski([1, 2, 3], [4, 6, 8], 2);
vector::distance::minkowski([1, 2], [1, 2, 3], 2);
vector::distance::minkowski([1, 2], [3, 4]);
vector::distance::minkowski([1, 2], [3, 4], "two");
