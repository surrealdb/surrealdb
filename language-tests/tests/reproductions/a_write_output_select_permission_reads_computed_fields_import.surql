/**
[test]
reason = "Setup for a_write_output_select_permission_reads_computed_fields.surql."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ hidden: false, id: doc:pub, status: 'public' }, { hidden: true, id: doc:sec, status: 'secret' }, { hidden: true, id: doc:sec2, status: 'secret' }, { hidden: false, id: doc:pub2, status: 'public' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: page:pub, status: 'public', visible: true }, { id: page:sec, status: 'secret', visible: false }]"

*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
    SIGNUP ( CREATE type::record('account', $id) )
    SIGNIN ( SELECT * FROM type::record('account', $id) )
    DURATION FOR TOKEN 1h, FOR SESSION 1h;

-- Deny-intended: the output of a write the table permits shows nothing the
-- caller may not select. Read as `NONE`, `hidden != true` shows every record.
DEFINE TABLE doc SCHEMALESS
    PERMISSIONS FOR select WHERE hidden != true, FOR create, update, delete FULL;
DEFINE FIELD hidden ON doc COMPUTED status = 'secret';

INSERT INTO doc [
    { id: doc:pub, status: 'public' },
    { id: doc:sec, status: 'secret' },
    { id: doc:sec2, status: 'secret' },
    { id: doc:pub2, status: 'public' }
];

-- Allow-intended. Read as `NONE`, `visible = true` shows no record.
DEFINE TABLE page SCHEMALESS
    PERMISSIONS FOR select WHERE visible = true, FOR create, update, delete FULL;
DEFINE FIELD visible ON page COMPUTED status = 'public';

INSERT INTO page [{ id: page:pub, status: 'public' }, { id: page:sec, status: 'secret' }];
