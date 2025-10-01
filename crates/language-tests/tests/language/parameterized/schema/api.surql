/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''{ "/test": "DEFINE API '/test' FOR any PERMISSIONS FULL COMMENT 'test comment'" }'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "{  }"

[env.capabilities]
allow-experimental = ["define_api"]

*/
LET $path = "/test";
LET $comment = "test comment";

DEFINE API $path COMMENT $comment;

(INFO FOR DATABASE).apis;

REMOVE API $path;

(INFO FOR DATABASE).apis;