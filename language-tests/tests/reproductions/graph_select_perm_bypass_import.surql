/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:alice }, { id: person:bob }, { id: person:dave }]"

[[test.results]]
value = "[{ id: friend:f_hidden, in: person:bob, out: person:dave }]"

*/

DEFINE TABLE person SCHEMALESS PERMISSIONS FOR select FULL;

DEFINE TABLE friend TYPE RELATION SCHEMALESS
    PERMISSIONS
        FOR select WHERE in.id = $auth.id
        FOR create FULL;

DEFINE ACCESS user ON DATABASE TYPE RECORD
    SIGNIN ( SELECT * FROM type::record('person', $id) )
    DURATION FOR TOKEN 1h, FOR SESSION 1h;

CREATE person:alice, person:bob, person:dave;
RELATE person:bob -> friend:f_hidden -> person:dave;
