/**
[env]
namespace = true
database = true

[test]
reason = """
Under OPTION IMPORT no view is maintained, so the COMPUTED values a view would
read are not derived either: a computed field reading a table the dump defines
later does not fail the import. Outside an import the same view is maintained
from the derived value.
"""

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
value = "[{ id: anchor:1, label: 'Lea', note: 'n' }]"

[[test.results]]
value = "[{ id: labelled:1, label: 'Lea' }]"
*/

OPTION IMPORT;
DEFINE FUNCTION fn::naming($e: record<anchor>) { RETURN (SELECT label FROM revision WHERE anchor = $e)[0]; };
DEFINE TABLE anchor SCHEMAFULL;
DEFINE FIELD label ON anchor COMPUTED fn::naming(id).label;
DEFINE FIELD note ON anchor TYPE option<string>;
DEFINE TABLE labelled AS SELECT label FROM anchor;
-- An export orders tables by name, so `anchor`'s rows arrive before the
-- `revision` table its computed field reads is defined.
INSERT [ { id: anchor:1 } ];
DEFINE TABLE revision SCHEMAFULL;
DEFINE FIELD anchor ON revision TYPE record<anchor>;
DEFINE FIELD label ON revision TYPE string;
INSERT [ { id: revision:1, anchor: anchor:1, label: 'Lea' } ];
-- Leaving import mode: the view is maintained again, from the derived value.
OPTION IMPORT = false;
UPDATE anchor:1 SET note = 'n';
SELECT * FROM labelled;
