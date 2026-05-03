/**
[test]
run = false
*/

DEFINE TABLE a SCHEMAFULL PERMISSIONS FULL;

DEFINE TABLE b SCHEMAFULL
    PERMISSIONS
        FOR create WHERE false
        FOR select FULL;

DEFINE FIELD a ON b TYPE record<a> REFERENCE;

CREATE a:test;

DEFINE ACCESS user ON DATABASE TYPE RECORD
    SIGNIN ( SELECT * FROM user WHERE id = type::record('user', $id) );

CREATE user:test;