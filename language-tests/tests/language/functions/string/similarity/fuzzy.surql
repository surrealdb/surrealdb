/**
[test]
reason = "Tests string::similarity::fuzzy (Smith-Waterman based) scoring including case-insensitivity, no-match zero score, and type errors"

[[test.results]]
value = "195"

[[test.results]]
value = "51"

[[test.results]]
value = "173"

[[test.results]]
value = "0"

[[test.results]]
value = "0"

[[test.results]]
value = "0"

[[test.results]]
value = "true"

[[test.results]]
error = "Incorrect arguments for function string::similarity::fuzzy(). Argument 1 was the wrong type. Expected `string` but found `123`"
*/

string::similarity::fuzzy("some text", "some text");
string::similarity::fuzzy("SurrealDB is a database", "srl");
string::similarity::fuzzy("SURREALDB", "surrealdb");
string::similarity::fuzzy("some text", "xyz");
string::similarity::fuzzy("", "");
string::similarity::fuzzy("text", "");
string::similarity::fuzzy("some text", "some text") == string::similarity::smithwaterman("some text", "some text");
string::similarity::fuzzy(123, "abc");
