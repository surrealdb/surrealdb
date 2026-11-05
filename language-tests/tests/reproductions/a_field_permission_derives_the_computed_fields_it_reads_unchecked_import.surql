/**
[test]
reason = "Setup for a_field_permission_derives_the_computed_fields_it_reads_unchecked.surql and its late-bound variant."
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
    SIGNUP ( CREATE type::record('account', $id) )
    SIGNIN ( SELECT * FROM type::record('account', $id) )
    DURATION FOR TOKEN 1h, FOR SESSION 1h;

-- Records the caller may not select.
DEFINE TABLE meta SCHEMALESS PERMISSIONS NONE;
CREATE meta:secret SET secret = true;
CREATE meta:open SET secret = false;

-- Every row of `doc` may be selected; its fields are guarded through computed
-- fields that read a record the caller may not select. Derived as the caller,
-- `hidden` is `NONE` and `public` is `false` on every row, so a guard written
-- to deny on `hidden` admits every row and one written to allow on `public`
-- admits none.
DEFINE TABLE doc SCHEMALESS PERMISSIONS FULL;
DEFINE FIELD hidden ON doc COMPUTED meta.secret;
DEFINE FIELD public ON doc COMPUTED meta.secret = false;
DEFINE FIELD body ON doc PERMISSIONS FOR select WHERE hidden != true;
DEFINE FIELD summary ON doc PERMISSIONS FOR select WHERE public = true;
-- The deny-intended guard on a stored field, which no view of the caller's
-- bends.
DEFINE FIELD note ON doc PERMISSIONS FOR select WHERE flag != true;
-- Computed fields' own guards reading another computed field, one judging
-- `$value` beside it.
DEFINE FIELD tag ON doc COMPUTED 'tagged' PERMISSIONS FOR select WHERE hidden != true;
DEFINE FIELD label ON doc COMPUTED 'labelled'
    PERMISSIONS FOR select WHERE $value = 'labelled' AND hidden != true;

CREATE doc:secret SET meta = meta:secret, flag = true,
    body = 'classified', summary = 'classified', note = 'classified';
CREATE doc:open SET meta = meta:open, flag = false,
    body = 'public', summary = 'public', note = 'public';

-- Records reaching the hidden one by a link and by an edge.
DEFINE TABLE holder SCHEMALESS PERMISSIONS FULL;
CREATE holder:1 SET ref = doc:secret;
DEFINE TABLE links TYPE RELATION SCHEMALESS PERMISSIONS FULL;
RELATE holder:1->links->doc:secret;

-- A guard on a nested path, one reading the row as a whole, and one on a
-- wildcard path, which a statement leaves to its scan to enforce, each through
-- the same kind of computed field.
DEFINE TABLE deep SCHEMALESS PERMISSIONS FULL;
DEFINE FIELD hidden ON deep COMPUTED meta.secret;
DEFINE FIELD card ON deep TYPE object;
DEFINE FIELD card.code ON deep PERMISSIONS FOR select WHERE hidden != true;
CREATE deep:secret SET meta = meta:secret, card = { code: 'classified', kind: 'memo' };
CREATE deep:open SET meta = meta:open, card = { code: 'public', kind: 'memo' };

DEFINE TABLE whole SCHEMALESS PERMISSIONS FULL;
DEFINE FIELD hidden ON whole COMPUTED meta.secret;
DEFINE FIELD remark ON whole PERMISSIONS FOR select WHERE $this.hidden != true;
CREATE whole:secret SET meta = meta:secret, remark = 'classified';
CREATE whole:open SET meta = meta:open, remark = 'public';

DEFINE TABLE wide SCHEMALESS PERMISSIONS FULL;
DEFINE FIELD hidden ON wide COMPUTED meta.secret;
DEFINE FIELD items ON wide TYPE array<object>;
DEFINE FIELD items.*.code ON wide PERMISSIONS FOR select WHERE hidden != true;
CREATE wide:secret SET meta = meta:secret,
    items = [{ code: 'classified', n: 1 }, { code: 'classified', n: 2 }];
CREATE wide:open SET meta = meta:open, items = [{ code: 'public', n: 1 }];

-- A computed field counting its own table, of which the caller may select one
-- row of two: derived as the caller it counts one, so a guard written to allow
-- on more than one admits no row.
DEFINE TABLE ring SCHEMALESS PERMISSIONS FOR select WHERE open = true, FOR create FULL;
DEFINE FIELD members ON ring COMPUTED count(SELECT VALUE id FROM ring);
DEFINE FIELD label ON ring PERMISSIONS FOR select WHERE members > 1;

CREATE ring:1 SET open = true, label = 'ringed';
CREATE ring:2 SET open = false, label = 'ringed';
