/**
[test]
reason = "Tests string::distance::normalized_damerau_levenshtein returning a 0.0-1.0 score where 1.0 means identical"

[[test.results]]
value = "1f"

[[test.results]]
value = "1f"

[[test.results]]
value = "0.5714285714285714f"

[[test.results]]
value = "0.5f"

[[test.results]]
value = "0f"

[[test.results]]
value = "0.8f"

[[test.results]]
error = "Incorrect arguments for function string::distance::normalized_damerau_levenshtein(). Argument 1 was the wrong type. Expected `string` but found `NONE`"
*/

string::distance::normalized_damerau_levenshtein("", "");
string::distance::normalized_damerau_levenshtein("kitten", "kitten");
string::distance::normalized_damerau_levenshtein("kitten", "sitting");
string::distance::normalized_damerau_levenshtein("ab", "ba");
string::distance::normalized_damerau_levenshtein("", "abc");
string::distance::normalized_damerau_levenshtein("héllo", "hello");
string::distance::normalized_damerau_levenshtein(NONE, "abc");
