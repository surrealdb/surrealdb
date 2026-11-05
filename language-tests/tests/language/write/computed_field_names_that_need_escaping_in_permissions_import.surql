/**
[test]
reason = "Setup for computed_field_names_that_need_escaping_in_permissions.surql."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """[{ id: doc:pub, "is hidden": false, status: 'public' }]"""

[[test.results]]
value = """[{ id: doc:sec, "is hidden": true, status: 'secret' }]"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """[{ "a-visible": true, id: page:pub, status: 'public', "z-withheld": false }]"""

[[test.results]]
value = """[{ "a-visible": false, id: page:sec, status: 'secret', "z-withheld": true }]"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """[{ balance: 10, id: acct:open, "is locked": false, note: 'n1', status: 'open' }]"""

[[test.results]]
value = """[{ balance: 10, id: acct:locked, "is locked": true, note: 'n2', status: 'locked' }]"""
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
    SIGNUP ( CREATE type::record('account', $id) )
    SIGNIN ( SELECT * FROM type::record('account', $id) )
    DURATION FOR TOKEN 1h, FOR SESSION 1h;

-- Deny-intended: a hidden document must not reach the caller. Read as `NONE`,
-- `!= true` admits every row.
DEFINE TABLE doc SCHEMALESS
    PERMISSIONS FOR select WHERE `is hidden` != true, FOR create, update FULL;
DEFINE FIELD `is hidden` ON doc COMPUTED status = 'secret';

CREATE doc:pub SET status = 'public';
CREATE doc:sec SET status = 'secret';

-- Allow-intended, through a name that sorts after the one that reads it. Read as
-- `NONE`, `= true` hides every row.
DEFINE TABLE page SCHEMALESS
    PERMISSIONS FOR select WHERE `a-visible` = true, FOR create, update FULL;
DEFINE FIELD `a-visible` ON page COMPUTED !`z-withheld`;
DEFINE FIELD `z-withheld` ON page COMPUTED status != 'public';

CREATE page:pub SET status = 'public';
CREATE page:sec SET status = 'secret';

-- A field's own permissions read a computed field of the record.
DEFINE TABLE acct SCHEMALESS PERMISSIONS FULL;
DEFINE FIELD `is locked` ON acct COMPUTED status = 'locked';
DEFINE FIELD note ON acct
    PERMISSIONS FOR select WHERE `is locked` = false, FOR create, update FULL;
DEFINE FIELD balance ON acct TYPE option<int>
    PERMISSIONS FOR select FULL, FOR create, update WHERE `is locked` = false;

CREATE acct:open SET status = 'open', note = 'n1', balance = 10;
CREATE acct:locked SET status = 'locked', note = 'n2', balance = 10;
