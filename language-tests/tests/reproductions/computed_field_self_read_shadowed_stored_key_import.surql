/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: account:alice }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ hidden: 'STALE_PHYSICAL', id: stale:one, note: 'NOTE' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ hidden: 'STALE_SECRET', id: redact:one }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: holder:one, link: redact:one }]"

*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
    SIGNUP ( CREATE type::record('account', $id) )
    SIGNIN ( SELECT * FROM type::record('account', $id) )
    DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE account SCHEMALESS PERMISSIONS FOR select, create FULL;
CREATE account:alice;

DEFINE TABLE stale SCHEMALESS PERMISSIONS FOR select, create FULL;
-- The value is stored physically while `hidden` is still an ordinary field, so
-- the key outlives the definition that follows it.
CREATE stale:one SET hidden = 'STALE_PHYSICAL', note = 'NOTE';
-- Redefining `hidden` as COMPUTED leaves the stored key shadowed: it is no
-- longer reachable through the field, but it is still in the record's bytes.
-- Because the stored permission pass classifies `hidden` as computed, it
-- reports no cut, so a "was anything cut" gate alone would skip re-derivation.
DEFINE FIELD hidden ON stale COMPUTED 'COMPUTED_VAL' PERMISSIONS FOR select NONE;
-- Readable, and reaches the shadowed key by dereferencing the record id.
DEFINE FIELD viaid ON stale COMPUTED id.hidden PERMISSIONS FOR select FULL;

-- The same shadowing on a table that restricts no field at all: the record user
-- is admitted by the table's own permission and every field is readable, so the
-- field-permission passes have nothing to do and return early. Redefining
-- `hidden` as a redacting COMPUTED field is the disclosure control here, so the
-- self-read must not hand back the value it replaced.
DEFINE TABLE redact SCHEMALESS PERMISSIONS FOR select, create FULL;
CREATE redact:one SET hidden = 'STALE_SECRET';
DEFINE FIELD hidden ON redact COMPUTED 'REDACTED' PERMISSIONS FOR select FULL;
DEFINE FIELD viaid ON redact COMPUTED id.hidden PERMISSIONS FOR select FULL;

-- A table permission predicate that dereferences a linked record. Predicates
-- are evaluated without applying permissions, so the linked record is computed
-- through the no-permission path rather than through the passes that apply
-- them. The shadowed key must not be the value `viaid` carries there either,
-- or this predicate admits a record it was written to deny.
DEFINE TABLE holder SCHEMALESS PERMISSIONS FOR select WHERE link.viaid = 'STALE_SECRET', FOR create FULL;
CREATE holder:one SET link = redact:one;
