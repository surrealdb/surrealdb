/**
[test]

[[test.results]]
value = "'www.cloud.surrealdb.com'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Incorrect arguments for function parse::url::domain(). Argument 1 was the wrong type. Expected `string` but found `1`"

*/
parse::url::domain("https://user:pass@www.cloud.surrealdb.com:80/path/to/page?query=param#somefragment");
parse::url::domain("www.bla.com");
parse::url::domain("bla");
parse::url::domain("");
parse::url::domain(1);
