/**
[test]
reason = "Tests string::distance::hamming including equal-length inputs, unicode, and the unequal-length error"

[[test.results]]
value = "0"

[[test.results]]
value = "0"

[[test.results]]
value = "3"

[[test.results]]
value = "1"

[[test.results]]
error = "Incorrect arguments for function string::distance::hamming(). Strings must be of equal length."

[[test.results]]
error = "Incorrect arguments for function string::distance::hamming(). Argument 2 was the wrong type. Expected `string` but found `NONE`"
*/

string::distance::hamming("", "");
string::distance::hamming("abc", "abc");
string::distance::hamming("karolin", "kathrin");
string::distance::hamming("äbc", "abc");
string::distance::hamming("abc", "abcd");
string::distance::hamming("abc", NONE);
