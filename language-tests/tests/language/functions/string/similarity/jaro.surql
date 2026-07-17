/**
[test]
reason = "Tests string::similarity::jaro returning a 0.0-1.0 score including empty strings, no-similarity, unicode, and type errors"

[[test.results]]
value = "1f"

[[test.results]]
value = "1f"

[[test.results]]
value = "0.9444444444444445f"

[[test.results]]
value = "0.7666666666666666f"

[[test.results]]
value = "0f"

[[test.results]]
value = "0f"

[[test.results]]
value = "0.8333333333333334f"

[[test.results]]
error = "Incorrect arguments for function string::similarity::jaro(). Argument 2 was the wrong type. Expected `string` but found `NONE`"
*/

string::similarity::jaro("", "");
string::similarity::jaro("martha", "martha");
string::similarity::jaro("martha", "marhta");
string::similarity::jaro("dixon", "dicksonx");
string::similarity::jaro("abc", "xyz");
string::similarity::jaro("", "abc");
string::similarity::jaro("café", "cafe");
string::similarity::jaro("abc", NONE);
