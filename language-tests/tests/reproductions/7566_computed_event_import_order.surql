/**
[env]
namespace = true
database = true

[test]
reason = "Under OPTION IMPORT no event runs, so the COMPUTED values an event would read are not derived either: a computed field reading a table the dump defines later does not fail the import"
issue = 7566

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
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ label: 'Lea' }]"

[[test.results]]
error = "Error while processing event guard: An error occurred: never"

*/

OPTION IMPORT;
DEFINE FUNCTION fn::naming($e: record<anchor>) { RETURN (SELECT label FROM revision WHERE anchor = $e)[0]; };
DEFINE TABLE anchor SCHEMAFULL;
DEFINE FIELD label ON anchor COMPUTED fn::naming(id).label;
DEFINE EVENT guard ON anchor WHEN $event = 'DELETE' THEN THROW 'never';
-- An export orders tables by name, so `anchor`'s rows arrive before the
-- `revision` table its computed field reads is defined.
INSERT [ { id: anchor:1 } ];
DEFINE TABLE revision SCHEMAFULL;
DEFINE FIELD anchor ON revision TYPE record<anchor>;
DEFINE FIELD label ON revision TYPE string;
INSERT [ { id: revision:1, anchor: anchor:1, label: 'Lea' } ];
-- Leaving import mode: the computed field and the event both apply again.
OPTION IMPORT = false;
SELECT label FROM anchor;
DELETE anchor:1;
