/**
[test]
reason = "Tests string::similarity::jaro_winkler returning a 0.0-1.0 score with common-prefix weighting, boundaries, and type errors"

[[test.results]]
value = "1f"

[[test.results]]
value = "1f"

[[test.results]]
value = "0.9611111111111111f"

[[test.results]]
value = "0.8133333333333332f"

[[test.results]]
value = "0.95f"

[[test.results]]
value = "0f"

[[test.results]]
value = "0f"

[[test.results]]
error = "Incorrect arguments for function string::similarity::jaro_winkler(). Argument 1 was the wrong type. Expected `string` but found `NULL`"
*/

string::similarity::jaro_winkler("", "");
string::similarity::jaro_winkler("martha", "martha");
string::similarity::jaro_winkler("martha", "marhta");
string::similarity::jaro_winkler("dixon", "dicksonx");
string::similarity::jaro_winkler("prefix", "prefixes");
string::similarity::jaro_winkler("abc", "xyz");
string::similarity::jaro_winkler("", "abc");
string::similarity::jaro_winkler(NULL, "abc");
