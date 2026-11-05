/**
[test]
reason = "Setup for a_record_user_of_an_undefined_table_reads_as_none_through_auth.surql."
run = false
*/
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( type::record($rid) ) DURATION FOR TOKEN 1h, FOR SESSION 1h;
DEFINE TABLE post SCHEMALESS PERMISSIONS FOR select WHERE author = $auth.id;
CREATE post:1 SET author = user:b;
CREATE post:2 SET author = user:c;
DEFINE TABLE post2 SCHEMALESS PERMISSIONS FOR select WHERE author = $auth.email;
CREATE post2:1 SET author = NONE;
CREATE post2:2 SET author = 'x';
DEFINE TABLE post3 SCHEMALESS PERMISSIONS FOR select WHERE $auth.email = NONE;
CREATE post3:1 SET a = 1;
