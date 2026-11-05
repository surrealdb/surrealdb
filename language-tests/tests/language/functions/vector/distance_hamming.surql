/**
[test]
reason = "Tests the vector::distance::hamming function"

[[test.results]]
value = "0"

[[test.results]]
value = "2"

[[test.results]]
value = "2"

[[test.results]]
value = "1"

[[test.results]]
value = "0"

[[test.results]]
error = "Incorrect arguments for function vector::distance::hamming(). The two vectors must be of the same dimension."

[[test.results]]
error = "Incorrect arguments for function vector::distance::hamming(). Argument 1 was the wrong type. Expected `number` but found `'a'` when coercing element at index 1 of `array<number>`"

[[test.results]]
error = "Incorrect arguments for function vector::distance::hamming(). Argument 1 was the wrong type. Expected `array<number>` but found `NONE`"
*/
vector::distance::hamming([1, 2, 3], [1, 2, 3]);
vector::distance::hamming([1, 2, 3], [1, 3, 2]);
vector::distance::hamming([10, 20, 30, 40], [10, 99, 30, 0]);
vector::distance::hamming([1, 2], [1.0, 2.5]);
vector::distance::hamming([], []);
vector::distance::hamming([1, 2], [1]);
vector::distance::hamming([1, "a"], [1, 2]);
vector::distance::hamming(NONE, [1, 2]);
