/**
[test]

[[test.results]]
value = "'https'"

[[test.results]]
value = "'foo'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Incorrect arguments for function parse::url::scheme(). Argument 1 was the wrong type. Expected `string` but found `1`"

*/
parse::url::scheme("https://user:pass@www.surrealdb.com:80/path/to/page?query=param#somefragment");
parse::url::scheme("foo://user:pass@www.surrealdb.com:80/path/to/page?query=param#somefragment");
parse::url::scheme("://user:pass@www.surrealdb.com:80/path/to/page?query=param#somefragment");
parse::url::scheme(" ://user:pass@www.surrealdb.com:80/path/to/page?query=param#somefragment");
parse::url::scheme("");
parse::url::scheme(1);
