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
value = '''{ test: "DEFINE USER test ON DATABASE PASSHASH '' ROLES OWNER DURATION FOR TOKEN 1h, FOR SESSION 2h COMMENT 'test'" }'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "{  }"

*/

LET $name = "test";
LET $token_duration = 1h;
LET $session_duration = 2h;
LET $comment = "test";

DEFINE USER $name ON DATABASE ROLES OWNER DURATION FOR TOKEN $token_duration FOR SESSION $session_duration COMMENT $comment;

(INFO FOR DB).users;

REMOVE USER $name ON DATABASE;
(INFO FOR DB).users;