/**
[test]
reason = "Tests string::similarity::smithwaterman scoring including case-insensitivity, no-match zero score, empty strings, and type errors"

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
error = "Incorrect arguments for function string::similarity::smithwaterman(). Argument 1 was the wrong type. Expected `string` but found `123`"
*/

string::similarity::smithwaterman("some text", "some text");
string::similarity::smithwaterman("SurrealDB is a database", "srl");
string::similarity::smithwaterman("SURREALDB", "surrealdb");
string::similarity::smithwaterman("some text", "xyz");
string::similarity::smithwaterman("", "");
string::similarity::smithwaterman("text", "");
string::similarity::smithwaterman(123, "abc");
