/**
[test]
reason = "Setup for a_field_write_permission_reads_computed_fields.surql."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ balance: 10, id: acct:locked, is_locked: true, status: 'locked' }]"

[[test.results]]
value = "[{ balance: 10, id: acct:open, is_locked: false, status: 'open' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ balance: 10, id: vault:locked, status: 'locked', unlocked: false, withheld: true }]"

[[test.results]]
value = "[{ balance: 10, id: vault:open, status: 'open', unlocked: true, withheld: false }]"

*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
    SIGNUP ( CREATE type::record('account', $id) )
    SIGNIN ( SELECT * FROM type::record('account', $id) )
    DURATION FOR TOKEN 1h, FOR SESSION 1h;

-- Deny-intended: `balance` may be written only while the account is not
-- locked. Read as `NONE`, `is_locked != true` admits every write.
DEFINE TABLE acct SCHEMALESS PERMISSIONS FULL;
DEFINE FIELD is_locked ON acct COMPUTED status = 'locked';
DEFINE FIELD balance ON acct PERMISSIONS FOR select FULL, FOR create, update WHERE is_locked != true;

CREATE acct:locked SET status = 'locked', balance = 10;
CREATE acct:open SET status = 'open', balance = 10;

-- Allow-intended, through a chain whose head sorts first by name. Read as
-- `NONE`, `unlocked = true` refuses every write; read off the record as the
-- caller wrote it, it admits whatever the caller assigns to `unlocked`.
DEFINE TABLE vault SCHEMALESS PERMISSIONS FULL;
DEFINE FIELD unlocked ON vault COMPUTED !withheld;
DEFINE FIELD withheld ON vault COMPUTED status != 'open';
DEFINE FIELD balance ON vault PERMISSIONS FOR select FULL, FOR create, update WHERE unlocked = true;

CREATE vault:locked SET status = 'locked', balance = 10;
CREATE vault:open SET status = 'open', balance = 10;
