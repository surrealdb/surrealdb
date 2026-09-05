/**
[test]

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
value = "[{ denied: 'S3CRET_TOKEN_VALUE', derived: 'S3CRET_TOKEN_VALUE', gated: 'S3CRET_TOKEN_VALUE', id: items:one, secret: 'S3CRET_TOKEN_VALUE' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ hidden: 'HIDDEN', id: bare:one, note: 'NOTE', shown: 'SHOWN' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: holders:one, link: items:one }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: account:alice }]"

[[test.results]]
value = "[{ id: owns:rel, in: account:alice, out: items:one }]"

*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
    SIGNUP ( CREATE type::record('account', $id) )
    SIGNIN ( SELECT * FROM type::record('account', $id) )
    DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE items SCHEMALESS PERMISSIONS FOR select, create FULL;
DEFINE FIELD secret ON items PERMISSIONS FOR select NONE, FOR create FULL;
-- `derived` is readable, but computes from a field that is not. It must not
-- carry that value once `secret` has been cut from the record.
DEFINE FIELD derived ON items COMPUTED secret PERMISSIONS FOR select FULL;
-- A computed field denied in its own right must stay absent, and must not be
-- reintroduced by re-deriving the computed fields against the reduced record.
DEFINE FIELD denied ON items COMPUTED secret PERMISSIONS FOR select NONE;
-- A computed field whose own clause reads `$value`. The clause must be answered
-- with the value the caller would receive — the one re-derived against the
-- reduced record — not with the value the field held before `secret` was cut,
-- or the field's presence in the result reports what `secret` holds.
DEFINE FIELD gated ON items COMPUTED secret PERMISSIONS FOR select WHERE $value != 'S3CRET_TOKEN_VALUE';

CREATE items:one SET secret = 'S3CRET_TOKEN_VALUE';

-- A table whose only restricted field is a COMPUTED one, so the stored pass
-- cuts nothing. The computed field's own clause still has to be applied, which
-- it would not be if a "nothing was cut" short-circuit skipped it.
DEFINE TABLE bare SCHEMALESS PERMISSIONS FOR select, create FULL;
DEFINE FIELD shown ON bare COMPUTED 'SHOWN' PERMISSIONS FOR select FULL;
DEFINE FIELD hidden ON bare COMPUTED 'HIDDEN' PERMISSIONS FOR select NONE;
CREATE bare:one SET note = 'NOTE';

-- A record link onto `items:one`, so dereferencing it resolves the record
-- through the single-record fetch path rather than through a scan.
DEFINE TABLE holders SCHEMALESS PERMISSIONS FOR select, create FULL;
CREATE holders:one SET link = items:one;

-- A graph edge onto `items:one`, so a traversal resolves it through the
-- batched linked-record paths rather than through a scan.
DEFINE TABLE account SCHEMALESS PERMISSIONS FOR select, create FULL;
DEFINE TABLE owns TYPE RELATION SCHEMALESS PERMISSIONS FOR select, create FULL;
CREATE account:alice;
RELATE account:alice->owns:rel->items:one;
