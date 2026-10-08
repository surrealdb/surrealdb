/**
[test]
reason = "Setup for a_table_select_permission_reads_computed_fields.surql."

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
value = "NONE"

[[test.results]]
value = "[{ id: page:pub, status: 'public', visible: true, withheld: false }]"

[[test.results]]
value = "[{ id: page:sec, status: 'secret', visible: false, withheld: true }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ doc: doc:sec, id: holder:1, page: page:pub }]"

[[test.results]]
value = "[{ id: links:1, in: holder:1, out: doc:sec }]"

[[test.results]]
value = "[{ id: links:2, in: holder:1, out: doc:pub }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: grant:1, who: 'mallory' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: squad:a, lead: 'mallory', owner: 'Mallory' }]"

[[test.results]]
value = "[{ id: squad:b, lead: 'bob', owner: 'Bob' }]"

*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
    SIGNUP ( CREATE type::record('account', $id) )
    SIGNIN ( SELECT * FROM type::record('account', $id) )
    DURATION FOR TOKEN 1h, FOR SESSION 1h;

-- Deny-intended: a hidden document must not reach the caller. Read as `NONE`,
-- `hidden != true` admits every row. The index gives the planner an access
-- path other than a table scan.
DEFINE TABLE doc SCHEMALESS
    PERMISSIONS FOR select WHERE hidden != true, FOR create, update FULL;
DEFINE FIELD hidden ON doc COMPUTED status = 'secret';
DEFINE INDEX doc_status ON doc FIELDS status;

CREATE doc:pub SET status = 'public';
CREATE doc:sec SET status = 'secret';

-- Allow-intended, through a chain whose head sorts first by name. Read as
-- `NONE`, `visible = true` hides every row.
DEFINE TABLE page SCHEMALESS
    PERMISSIONS FOR select WHERE visible = true, FOR create, update FULL;
DEFINE FIELD visible ON page COMPUTED !withheld;
DEFINE FIELD withheld ON page COMPUTED status != 'public';

CREATE page:pub SET status = 'public';
CREATE page:sec SET status = 'secret';

-- Records that reach `doc` and `page` through a link and a graph edge.
DEFINE TABLE holder SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE links TYPE RELATION SCHEMALESS PERMISSIONS FULL;
CREATE holder:1 SET doc = doc:sec, page = page:pub;
RELATE holder:1->links:1->doc:sec;
RELATE holder:1->links:2->doc:pub;

-- Deny-intended membership over a computed field: a record whose lead holds a
-- grant is hidden. Read as `NONE`, `lead NOT IN (…)` admits every row.
DEFINE TABLE grant SCHEMALESS PERMISSIONS FULL;
CREATE grant:1 SET who = 'mallory';
DEFINE TABLE squad SCHEMALESS
    PERMISSIONS FOR select WHERE lead NOT IN (SELECT VALUE who FROM grant), FOR create, update FULL;
DEFINE FIELD lead ON squad COMPUTED string::lowercase(owner);
CREATE squad:a SET owner = 'Mallory';
CREATE squad:b SET owner = 'Bob';
