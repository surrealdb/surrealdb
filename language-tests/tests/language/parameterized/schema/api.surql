/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ "/test": "DEFINE API '/test' FOR any PERMISSIONS FULL COMMENT 'test comment'" }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ "/test": "DEFINE API '/test' FOR any PERMISSIONS WHERE role = $user_role COMMENT 'test comment'" }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ "/test2": "DEFINE API '/test2' FOR any MIDDLEWARE fn::middleware('auth') PERMISSIONS FULL COMMENT 'test comment'" }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "{  }"

[env.capabilities]
allow-experimental = ["define_api"]
*/
LET $path = "/test";
LET $path2 = "/test2";
LET $comment = "test comment";
LET $user_role = "admin";
LET $middleware_arg = "auth";

-- Test basic DEFINE API with parameterized path and comment
DEFINE API $path COMMENT $comment;

(INFO FOR DATABASE).apis;

REMOVE API $path;

-- Test DEFINE API with PERMISSIONS WHERE using parameter
DEFINE API $path FOR any PERMISSIONS WHERE role = $user_role COMMENT $comment;

(INFO FOR DATABASE).apis;

REMOVE API $path;

-- Test DEFINE API with MIDDLEWARE using parameterized function call
DEFINE API $path2 FOR any MIDDLEWARE fn::middleware($middleware_arg) COMMENT $comment;

(INFO FOR DATABASE).apis;

REMOVE API $path2;

(INFO FOR DATABASE).apis;