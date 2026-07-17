/**
[test]
reason = "Tests the vector::similarity::jaccard function"

[[test.results]]
value = "1f"

[[test.results]]
value = "0f"

[[test.results]]
value = "0.5f"

[[test.results]]
value = "0.2f"

[[test.results]]
value = "0f"

[[test.results]]
value = "NaN"

[[test.results]]
error = "Incorrect arguments for function vector::similarity::jaccard(). Argument 2 was the wrong type. Expected `array<number>` but found `NONE`"
*/
vector::similarity::jaccard([1, 2, 3], [1, 2, 3]);
vector::similarity::jaccard([1, 2, 3], [4, 5, 6]);
vector::similarity::jaccard([1, 2, 3], [2, 3, 4]);
vector::similarity::jaccard([1, 2], [2, 3, 4, 5]);
vector::similarity::jaccard([], [1, 2]);
vector::similarity::jaccard([], []);
vector::similarity::jaccard([1, 2], NONE);
