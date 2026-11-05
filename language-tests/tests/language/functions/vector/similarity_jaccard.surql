/**
[test]
reason = "Tests the vector::similarity::jaccard function: the weighted (Ruzicka) Jaccard similarity Σmin/Σmax, positional, over vectors of equal dimension"

[[test.results]]
value = "1f"

[[test.results]]
value = "0.4f"

[[test.results]]
value = "0.6666666666666666f"

[[test.results]]
error = "Incorrect arguments for function vector::similarity::jaccard(). The two vectors must be of the same dimension."

[[test.results]]
value = "0.75f"

[[test.results]]
error = "Incorrect arguments for function vector::similarity::jaccard(). The two vectors must be of the same dimension."

[[test.results]]
value = "1f"

[[test.results]]
error = "Incorrect arguments for function vector::similarity::jaccard(). Argument 2 was the wrong type. Expected `array<number>` but found `NONE`"
*/
vector::similarity::jaccard([1, 2, 3], [1, 2, 3]);
vector::similarity::jaccard([1, 2, 3], [4, 5, 6]);
vector::similarity::jaccard([1, 2, 3], [2, 3, 4]);
vector::similarity::jaccard([1, 2], [2, 3, 4, 5]);
vector::similarity::jaccard([1, 2], [2, 2]);
vector::similarity::jaccard([], [1, 2]);
vector::similarity::jaccard([], []);
vector::similarity::jaccard([1, 2], NONE);
