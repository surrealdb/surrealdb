/**
[test]
reason = "Tests string::distance::levenshtein including empty strings, unicode, no-transposition behavior, and type errors"

[[test.results]]
value = "0"

[[test.results]]
value = "0"

[[test.results]]
value = "3"

[[test.results]]
value = "2"

[[test.results]]
value = "3"

[[test.results]]
value = "3"

[[test.results]]
value = "1"

[[test.results]]
error = "Incorrect arguments for function string::distance::levenshtein(). Argument 1 was the wrong type. Expected `string` but found `true`"
*/

string::distance::levenshtein("", "");
string::distance::levenshtein("kitten", "kitten");
string::distance::levenshtein("kitten", "sitting");
string::distance::levenshtein("flaw", "lawn");
string::distance::levenshtein("ca", "abc");
string::distance::levenshtein("", "abc");
string::distance::levenshtein("café", "cafe");
string::distance::levenshtein(true, "abc");
