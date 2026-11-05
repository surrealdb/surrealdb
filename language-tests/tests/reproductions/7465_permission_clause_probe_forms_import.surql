/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Setup for 7465_permission_clause_probe_forms.surql and 7466_permission_clause_graph_traversal.surql: a lookup table and an edge table the caller can never read, and tables whose permission clauses probe them in each spelling."
run = false
*/

DEFINE TABLE person SCHEMALESS PERMISSIONS NONE;
CREATE person:bob;
DEFINE TABLE lookup SCHEMALESS PERMISSIONS NONE;
CREATE lookup:[person:bob, 'ok'];

DEFINE TABLE item_a SCHEMALESS PERMISSIONS
	FOR select WHERE lookup:[$auth, 'ok'].id != NONE
	FOR create NONE FOR update NONE FOR delete NONE;
DEFINE TABLE item_b SCHEMALESS PERMISSIONS
	FOR select WHERE array::len((SELECT VALUE id FROM lookup WHERE id = lookup:[$auth, 'ok'])) > 0
	FOR create NONE FOR update NONE FOR delete NONE;
DEFINE TABLE item_c SCHEMALESS PERMISSIONS
	FOR select WHERE record::exists(lookup:[$auth, 'ok'])
	FOR create WHERE record::exists(lookup:[$auth, 'ok'])
	FOR update NONE FOR delete NONE;
CREATE item_a:one, item_b:one, item_c:one;

DEFINE TABLE linked TYPE RELATION PERMISSIONS NONE;
DEFINE TABLE doc SCHEMALESS PERMISSIONS
	FOR select WHERE $auth INSIDE <-linked<-person
	FOR create NONE FOR update NONE FOR delete NONE;
CREATE doc:one, doc:two;
RELATE person:bob->linked->doc:one;
