/**
[test]
reason = "Setup for `missing_table_record_user`: a record access and the user it signs in."
run = false
*/
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid ) DURATION FOR TOKEN 1h, FOR SESSION 1h;
DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;
CREATE person:bob SET lim = 8;
