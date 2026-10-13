/**
[test]
reason = "Tests string::distance::normalized_levenshtein returning a 0.0-1.0 score where 1.0 means identical"

[[test.results]]
value = "1f"

[[test.results]]
value = "1f"

[[test.results]]
value = "0.5714285714285714f"

[[test.results]]
value = "0f"

[[test.results]]
value = "0f"

[[test.results]]
value = "0.75f"

[[test.results]]
error = "Incorrect arguments for function string::distance::normalized_levenshtein(). Argument 1 was the wrong type. Expected `string` but found `NULL`"
*/

string::distance::normalized_levenshtein("", "");
string::distance::normalized_levenshtein("kitten", "kitten");
string::distance::normalized_levenshtein("kitten", "sitting");
string::distance::normalized_levenshtein("ca", "abc");
string::distance::normalized_levenshtein("", "abc");
string::distance::normalized_levenshtein("café", "cafe");
string::distance::normalized_levenshtein(NULL, "abc");
