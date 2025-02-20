/**
[test]

[[test.results]]
value = "'query=param'"

[[test.results]]
value = "'query=param,foo=bar,faz=%27with%20space%27'"

*/
parse::url::query("https://user:pass@www.surrealdb.com:80/path/to/page?query=param#somefragment");
parse::url::query("https://user:pass@www.surrealdb.com:80/path/to/page?query=param,foo=bar,faz='with space'#somefragment");
