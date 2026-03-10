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
value = "NONE"

[[test.results]]
value = """{ testac: "DEFINE ACCESS testac ON NAMESPACE TYPE JWT ALGORITHM HS256 KEY '[REDACTED]' WITH ISSUER KEY '[REDACTED]' DURATION FOR TOKEN 1h, FOR SESSION NONE COMMENT 'testac comment'" }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ testac: "DEFINE ACCESS testac ON NAMESPACE TYPE JWT ALGORITHM HS256 KEY '[REDACTED]' WITH ISSUER KEY '[REDACTED]' DURATION FOR TOKEN 1h, FOR SESSION 2h COMMENT 'testac comment'" }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "{  }"
*/

LET $name = "testac";
LET $comment = "testac comment";
LET $key = "foo";
LET $same_key = "foo";
LET $token_duration = 1h;
LET $session_duration = 2h;

-- Test with basic KEY parameter
DEFINE ACCESS $name ON NAMESPACE TYPE JWT ALGORITHM HS256 KEY $key COMMENT $comment;

(INFO FOR NAMESPACE).accesses;

REMOVE ACCESS $name ON NAMESPACE;

-- Test with WITH ISSUER KEY (same value as KEY) and DURATION parameters
-- For symmetric algorithms, the issuer key must match the verifier key
DEFINE ACCESS $name ON NAMESPACE TYPE JWT ALGORITHM HS256 KEY $key WITH ISSUER KEY $same_key DURATION FOR TOKEN $token_duration FOR SESSION $session_duration COMMENT $comment;

(INFO FOR NAMESPACE).accesses;

REMOVE ACCESS $name ON NAMESPACE;

(INFO FOR NAMESPACE).accesses;
