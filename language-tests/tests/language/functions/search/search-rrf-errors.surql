/**
[test]

[[test.results]]
error = "Incorrect arguments for function search::rrf(). limit must be at least 1"

[[test.results]]
error = "Incorrect arguments for function search::rrf(). limit must be at least 1"

[[test.results]]
value = "[]"

[[test.results]]
error = "Incorrect arguments for function search::rrf(). RRF constant must be at least 0"

[[test.results]]
value = "[]"

*/

RETURN search::rrf([], 0);
RETURN search::rrf([], -1);
RETURN search::rrf([], 1);
RETURN search::rrf([], 1, -1);
RETURN search::rrf([], 1, 0);
