/**
[test]
reason = "Tests the 3D vector cross product, including float inputs, zero vectors, and non-3D dimension errors"

[[test.results]]
value = "[-3, 6, -3]"

[[test.results]]
value = "[0, 0, 1]"

[[test.results]]
value = "[-3f, 6f, -3f]"

[[test.results]]
value = "[0, 0, 0]"

[[test.results]]
error = "Incorrect arguments for function vector::cross(). Both vectors must have a dimension of 3."

[[test.results]]
error = "Incorrect arguments for function vector::cross(). Both vectors must have a dimension of 3."

[[test.results]]
error = "Incorrect arguments for function vector::cross(). Both vectors must have a dimension of 3."
*/
vector::cross([1, 2, 3], [4, 5, 6]);
vector::cross([1, 0, 0], [0, 1, 0]);
vector::cross([1.0, 2.0, 3.0], [4.0, 5.0, 6.0]);
vector::cross([0, 0, 0], [1, 2, 3]);
vector::cross([1, 2], [3, 4]);
vector::cross([1, 2, 3], [4, 5]);
vector::cross([1, 2, 3, 4], [5, 6, 7, 8]);
