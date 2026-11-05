/**
[test]
reason = "Tests summing a collection of vectors elementwise, including mixed numeric types, empty vectors, an empty collection, dimension mismatches, and invalid input types"

[[test.results]]
value = "[5, 7, 9]"

[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
value = "[9, 12]"

[[test.results]]
value = "[2.5f, 3.5f]"

[[test.results]]
value = "[2.5dec, 4.5dec]"

[[test.results]]
value = "[0, 0]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Incorrect arguments for function vector::sum(). The two vectors must be of the same dimension."

[[test.results]]
error = "Incorrect arguments for function vector::sum(). Argument 1 was the wrong type. Expected `array<number>` but found `1` when coercing element at index 0 of `array<array<number>>`"

[[test.results]]
error = "Incorrect arguments for function vector::sum(). Argument 1 was the wrong type. Expected `array<array<number>>` but found `'nope'`"

[[test.results]]
error = "Incorrect arguments for function vector::sum(). Argument 1 was the wrong type. Expected `number` but found `'a'` when coercing element at index 1 of `array<number>` when coercing element at index 0 of `array<array<number>>`"

[[test.results]]
error = "Incorrect arguments for function vector::sum(). Argument 1 was the wrong type. Expected `array<array<number>>` but found `NONE`"

[[test.results]]
value = "[4, 6]"
*/
vector::sum([[1, 2, 3], [4, 5, 6]]);
vector::sum([[1, 2, 3]]);
vector::sum([[1, 2], [3, 4], [5, 6]]);
vector::sum([[1.5, 2.5], [1, 1]]);
vector::sum([[1, 2], [1.5dec, 2.5dec]]);
vector::sum([[-1, -2], [1, 2]]);
vector::sum([[], []]);
vector::sum([]);
vector::sum([[1, 2], [1, 2, 3]]);
vector::sum([1, 2, 3]);
vector::sum("nope");
vector::sum([[1, "a"]]);
vector::sum(NONE);
[[1, 2], [3, 4]].vector_sum();
