/**
[test]
reason = "Tests the vector::similarity::pearson function"

[[test.results]]
value = "1f"

[[test.results]]
value = "-1f"

[[test.results]]
value = "0.9999999999999998f"

[[test.results]]
value = "0.7999999999999998f"

[[test.results]]
value = "NaN"

[[test.results]]
value = "NaN"

[[test.results]]
error = "Incorrect arguments for function vector::similarity::pearson(). The two vectors must be of the same dimension."

[[test.results]]
error = "Incorrect arguments for function vector::similarity::pearson(). Argument 1 was the wrong type. Expected `array<number>` but found `'a'`"
*/
vector::similarity::pearson([1, 2, 3], [1, 2, 3]);
vector::similarity::pearson([1, 2, 3], [3, 2, 1]);
vector::similarity::pearson([1, 2, 3, 4, 5], [2, 4, 6, 8, 10]);
vector::similarity::pearson([1, 2, 3, 4, 5], [1, 3, 2, 5, 4]);
vector::similarity::pearson([1, 1, 1], [1, 2, 3]);
vector::similarity::pearson([], []);
vector::similarity::pearson([1, 2], [1, 2, 3]);
vector::similarity::pearson("a", [1, 2]);
