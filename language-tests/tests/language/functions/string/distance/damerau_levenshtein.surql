/**
[test]
reason = "Tests string::distance::damerau_levenshtein including transpositions, unicode, empty strings, and type errors"

[[test.results]]
value = "0"

[[test.results]]
value = "0"

[[test.results]]
value = "3"

[[test.results]]
value = "1"

[[test.results]]
value = "2"

[[test.results]]
value = "3"

[[test.results]]
value = "1"

[[test.results]]
error = "Incorrect arguments for function string::distance::damerau_levenshtein(). Argument 1 was the wrong type. Expected `string` but found `123`"
*/

string::distance::damerau_levenshtein("", "");
string::distance::damerau_levenshtein("kitten", "kitten");
string::distance::damerau_levenshtein("kitten", "sitting");
string::distance::damerau_levenshtein("ab", "ba");
string::distance::damerau_levenshtein("ca", "abc");
string::distance::damerau_levenshtein("", "abc");
string::distance::damerau_levenshtein("héllo", "hello");
string::distance::damerau_levenshtein(123, "abc");
