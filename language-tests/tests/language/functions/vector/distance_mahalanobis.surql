/**
[test]
reason = "Tests the vector::distance::mahalanobis function with identity and correlated covariance matrices, plus dimension, shape, and positive-definiteness errors"

[[test.results]]
value = "2.8284271247461903f"

[[test.results]]
value = "1.632993161855452f"

[[test.results]]
value = "2.179449471770337f"

[[test.results]]
value = "0f"

[[test.results]]
error = "Incorrect arguments for function vector::distance::mahalanobis(). The two vectors must be of the same dimension."

[[test.results]]
error = "Incorrect arguments for function vector::distance::mahalanobis(). The two vectors must not be empty."

[[test.results]]
error = "Incorrect arguments for function vector::distance::mahalanobis(). The covariance matrix must be a square matrix of the same dimension as the vectors."

[[test.results]]
error = "Incorrect arguments for function vector::distance::mahalanobis(). The covariance matrix must be symmetric positive-definite."

[[test.results]]
error = "Incorrect arguments for function vector::distance::mahalanobis(). The covariance matrix must be symmetric positive-definite."

[[test.results]]
error = "Incorrect arguments for function vector::distance::mahalanobis(). Argument 3 was the wrong type. Expected `array<array<number>>` but found `'abc'`"
*/
vector::distance::mahalanobis([1, 2], [3, 4], [[1, 0], [0, 1]]);
vector::distance::mahalanobis([1, 2], [3, 4], [[2, 1], [1, 2]]);
vector::distance::mahalanobis([1, 2, 3], [2, 4, 6], [[4, 2, 0], [2, 3, 1], [0, 1, 2]]);
vector::distance::mahalanobis([1, 2], [1, 2], [[2, 1], [1, 2]]);
vector::distance::mahalanobis([1, 2], [1, 2, 3], [[1, 0], [0, 1]]);
vector::distance::mahalanobis([], [], []);
vector::distance::mahalanobis([1, 2], [3, 4], [[1, 0]]);
vector::distance::mahalanobis([1, 2], [3, 4], [[1, 2], [3, 4]]);
vector::distance::mahalanobis([1, 2], [3, 4], [[1, 2], [2, 1]]);
vector::distance::mahalanobis([1, 2], [3, 4], "abc");
