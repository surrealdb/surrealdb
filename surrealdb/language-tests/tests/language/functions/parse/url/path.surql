/**
[test]

[[test.results]]
value = "'/path/to/page'"

[[test.results]]
value = "'80/path/to/page'"

[[test.results]]
value = "'80/'"

[[test.results]]
value = "'/'"

[[test.results]]
value = "'/'"

[[test.results]]
value = "'/'"

[[test.results]]
value = "'/'"

[[test.results]]
value = "'/.../'"

[[test.results]]
value = "'/'"

[[test.results]]
value = "'/baz/'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Incorrect arguments for function parse::url::path(). Argument 1 was the wrong type. Expected `string` but found `1`"

*/
parse::url::path("https://user:pass@www.surrealdb.com:80/path/to/page?query=param#somefragment");
parse::url::path("www.surrealdb.com:80/path/to/page?query=param#somefragment");
parse::url::path("www.surrealdb.com:80/");
parse::url::path("https://www.surrealdb.com/");
parse::url::path("https://www.surrealdb.com");
parse::url::path("https://www.surrealdb.com/./");
parse::url::path("https://www.surrealdb.com/../");
parse::url::path("https://www.surrealdb.com/.../");
parse::url::path("https://www.surrealdb.com/foo/../");
parse::url::path("https://www.surrealdb.com/foo/../bar/../baz/./");
parse::url::path("/foo/../bar/../baz/./");
parse::url::path("");
parse::url::path(1);
