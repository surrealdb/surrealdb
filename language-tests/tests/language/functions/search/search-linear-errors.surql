/**
[test]

[[test.results]]
error = "Incorrect arguments for function search::linear(). Limit must be at least 1"

[[test.results]]
error = "Incorrect arguments for function search::linear(). Limit must be at least 1"

[[test.results]]
value = "[]"

[[test.results]]
error = "Incorrect arguments for function search::linear(). Norm must be 'minmax' or 'zscore'"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Incorrect arguments for function search::linear(). The results and the weights array should have the same length"

[[test.results]]
error = "Incorrect arguments for function search::linear(). Weight at index 0 must be a number"

*/

RETURN search::linear([], [], 0, "minmax");
RETURN search::linear([], [], -1, "minmax");
RETURN search::linear([], [], 1, "minmax");
RETURN search::linear([], [], 1, "dummy");
RETURN search::linear([], [], 1, "minmax");
RETURN search::linear([], [], 1, "zscore");
RETURN search::linear([], [1], 1, "zscore");
RETURN search::linear([[]], ["a"], 1, "zscore");
