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
error = "Incorrect arguments for function parse::url::host(). Argument 1 was the wrong type. Expected `string` but found `1`"

*/
parse::url::host("https://user:pass@www.cloud.surrealdb.com:80/path/to/page?query=param#somefragment");
parse::url::host("www.bla.com");
parse::url::host("bla");
parse::url::host("");
parse::url::host(1);
