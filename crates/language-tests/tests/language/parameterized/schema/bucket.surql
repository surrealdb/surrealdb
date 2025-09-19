/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''{ test: "DEFINE BUCKET test BACKEND 'memory' PERMISSIONS FULL COMMENT 'test comment'" }'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "{  }"

[env.capabilities]
allow-experimental = ["files"]

*/
LET $name = "test";
LET $comment = "test comment";

DEFINE BUCKET $name BACKEND "memory" COMMENT $comment;

(INFO FOR DATABASE).buckets;

REMOVE BUCKET $name;

(INFO FOR DATABASE).buckets;