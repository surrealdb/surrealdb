/**
[test]
reason = "Setup for a_permission_reads_computed_fields_through_a_function.surql."

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
value = "[{ hidden: false, id: doc:pub, status: 'public' }]"

[[test.results]]
value = "[{ hidden: true, id: doc:sec, status: 'secret' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ hidden: false, id: note:pub, status: 'public' }]"

[[test.results]]
value = "[{ hidden: true, id: note:sec, status: 'secret' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: acct:locked, is_locked: true, status: 'locked' }]"

[[test.results]]
value = "[{ id: acct:open, is_locked: false, status: 'open' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ balance: 10, id: vault:locked, is_locked: true, status: 'locked' }]"

[[test.results]]
value = "[{ balance: 10, id: vault:open, is_locked: false, status: 'open' }]"

*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
    SIGNUP ( CREATE type::record('account', $id) )
    SIGNIN ( SELECT * FROM type::record('account', $id) )
    DURATION FOR TOKEN 1h, FOR SESSION 1h;

-- Functions whose bodies read a COMPUTED field off the record they are called
-- against. Neither names the field in its arguments.
DEFINE FUNCTION fn::hidden() { RETURN hidden; };
DEFINE FUNCTION fn::locked() { RETURN is_locked; };

-- Deny-intended table SELECT through a user-defined function. Read as `NONE`,
-- `fn::hidden() != true` admits every row.
DEFINE TABLE doc SCHEMALESS
    PERMISSIONS FOR select WHERE fn::hidden() != true, FOR create, update FULL;
DEFINE FIELD hidden ON doc COMPUTED status = 'secret';

CREATE doc:pub SET status = 'public';
CREATE doc:sec SET status = 'secret';

-- The same through a builtin that reads a field by a name it is handed.
DEFINE TABLE note SCHEMALESS
    PERMISSIONS FOR select WHERE type::field('hidden') != true, FOR create, update FULL;
DEFINE FIELD hidden ON note COMPUTED status = 'secret';

CREATE note:pub SET status = 'public';
CREATE note:sec SET status = 'secret';

-- Deny-intended table write gates through a user-defined function.
DEFINE TABLE acct SCHEMALESS
    PERMISSIONS FOR select FULL, FOR create, update, delete WHERE fn::locked() != true;
DEFINE FIELD is_locked ON acct COMPUTED status = 'locked';

CREATE acct:locked SET status = 'locked';
CREATE acct:open SET status = 'open';

-- Deny-intended field write permission through a user-defined function.
DEFINE TABLE vault SCHEMALESS PERMISSIONS FULL;
DEFINE FIELD is_locked ON vault COMPUTED status = 'locked';
DEFINE FIELD balance ON vault PERMISSIONS FOR select FULL, FOR create, update WHERE fn::locked() != true;

CREATE vault:locked SET status = 'locked', balance = 10;
CREATE vault:open SET status = 'open', balance = 10;
