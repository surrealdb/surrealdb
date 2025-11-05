/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''{ testac: "DEFINE ANALYZER testac COMMENT 'testac comment'" }'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "{  }"

*/

LET $name = "testac";
LET $comment = "testac comment";

DEFINE ANALYZER $name COMMENT $comment;

(INFO FOR DATABASE).analyzers;

REMOVE ANALYZER $name;

(INFO FOR DATABASE).analyzers;