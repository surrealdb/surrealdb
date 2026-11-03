/**
[test]
run = false
*/

DEFINE TABLE a SCHEMAFULL PERMISSIONS FULL;

DEFINE TABLE b SCHEMAFULL
    PERMISSIONS
        FOR update WHERE false
        FOR select, create FULL;

DEFINE FIELD a ON b TYPE record<a> REFERENCE;

CREATE a:test;
CREATE a:test2;

DEFINE ACCESS user ON DATABASE TYPE RECORD
    SIGNIN ( SELECT * FROM user WHERE id = type::record('user', $id) );

CREATE user:test;

-- Create b:test as root with a reference to a:test
CREATE b:test CONTENT { a: a:test };
