/**
[test]
reason = "Tests the vector::similarity::spearman function with monotonic, inverse, and tied inputs, plus dimension and variance errors"

[[test.results]]
value = "1f"

[[test.results]]
value = "-1f"

[[test.results]]
value = "0.9999999999999998f"

[[test.results]]
value = "0.9486832980505138f"

[[test.results]]
error = "Incorrect arguments for function vector::similarity::spearman(). The two vectors must be of the same dimension."

[[test.results]]
error = "Incorrect arguments for function vector::similarity::spearman(). The two vectors must have a dimension of at least 2."

[[test.results]]
error = "Incorrect arguments for function vector::similarity::spearman(). The two vectors must not have a variance of zero."

[[test.results]]
error = "Incorrect arguments for function vector::similarity::spearman(). Argument 2 was the wrong type. Expected `array<number>` but found `NONE`"
*/
vector::similarity::spearman([1, 2, 3], [1, 10, 100]);
vector::similarity::spearman([1, 2, 3], [3, 2, 1]);
vector::similarity::spearman([1, 2, 3, 4, 5], [2, 20, 60, 80, 100]);
vector::similarity::spearman([1, 2, 2, 3], [1, 2, 3, 4]);
vector::similarity::spearman([1, 2], [1, 2, 3]);
vector::similarity::spearman([1], [1]);
vector::similarity::spearman([2, 2, 2], [1, 2, 3]);
vector::similarity::spearman([1, 2], NONE);
