/**
[test]

[[test.results]]
value = "80"

[[test.results]]
value = "65535"

[[test.results]]
value = "NONE"

*/
parse::url::port("https://user:pass@www.surrealdb.com:80/path/to/page?query=param#somefragment");
parse::url::port("https://user:pass@www.surrealdb.com:65535/path/to/page?query=param#somefragment");
parse::url::port("https://user:pass@www.surrealdb.com:65536/path/to/page?query=param#somefragment");
