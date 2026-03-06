/**
[test]
reason = "Setup: tables and fields with field-level permissions for record user tests"

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
value = "[{ id: item:1, locked: 'preset', name: 'alpha', owner: user:1, readonly: 'fixed', secret: 'hidden' }]"

[[test.results]]
value = "[{ id: item:2, locked: 'preset2', name: 'beta', owner: user:2, readonly: 'fixed2', secret: 'other_hidden' }]"

[[test.results]]
value = "[{ id: user:1 }]"

[[test.results]]
value = "[{ id: user:2 }]"

*/
DEFINE ACCESS test ON DATABASE TYPE RECORD SIGNUP (CREATE type::record('user', $id)) SIGNIN (SELECT * FROM type::record('user', $id));

DEFINE TABLE item PERMISSIONS FULL;
DEFINE FIELD name ON item TYPE string;
DEFINE FIELD secret ON item TYPE string PERMISSIONS FOR select WHERE owner = $auth;
DEFINE FIELD locked ON item TYPE string PERMISSIONS FOR create NONE;
DEFINE FIELD readonly ON item TYPE string PERMISSIONS FOR update NONE;
DEFINE FIELD owner ON item TYPE record<user>;

CREATE item:1 SET name = 'alpha', secret = 'hidden', locked = 'preset', readonly = 'fixed', owner = user:1;
CREATE item:2 SET name = 'beta', secret = 'other_hidden', locked = 'preset2', readonly = 'fixed2', owner = user:2;
CREATE user:1;
CREATE user:2;
