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
value = '''{ testac: "DEFINE ACCESS testac ON NAMESPACE TYPE JWT ALGORITHM HS256 KEY '[REDACTED]' WITH ISSUER KEY '[REDACTED]' DURATION FOR TOKEN 1h, FOR SESSION NONE COMMENT 'testac comment'" }'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "{  }"

*/

LET $name = "testac";
LET $comment = "testac comment";
LET $key = "foo";

DEFINE ACCESS $name ON NAMESPACE TYPE JWT ALGORITHM HS256 KEY $key COMMENT $comment;

(INFO FOR NAMESPACE).accesses;

REMOVE ACCESS $name ON NAMESPACE;

(INFO FOR NAMESPACE).accesses;