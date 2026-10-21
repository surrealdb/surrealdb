/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Setup for 7465_permission_clause_probe_forms_every_clause.surql: a lookup table and an edge table the caller can never read, and for each spelling of a probe of them a table whose every clause, and a field whose select clause, is that probe. Each spelling has a twin probing for a record that does not exist, as a table and as a second field of the first table, and each table has a field whose create and update clauses are the probe and one whose clauses are its twin."
run = false
*/

DEFINE TABLE person SCHEMALESS PERMISSIONS NONE;
CREATE person:bob;
DEFINE TABLE lookup SCHEMALESS PERMISSIONS NONE;
CREATE lookup:[person:bob, 'ok'];
DEFINE TABLE gate SCHEMALESS PERMISSIONS NONE;
CREATE gate:ok, gate:no;
DEFINE TABLE linked TYPE RELATION PERMISSIONS NONE;
RELATE person:bob->linked->gate:ok;

DEFINE TABLE deref_ok SCHEMALESS PERMISSIONS FOR select, create, update, delete WHERE lookup:[$auth, 'ok'].id != NONE;
DEFINE FIELD secret ON deref_ok PERMISSIONS FOR select WHERE lookup:[$auth, 'ok'].id != NONE;
DEFINE FIELD hidden ON deref_ok PERMISSIONS FOR select WHERE lookup:[$auth, 'no'].id != NONE;
DEFINE FIELD w_ok ON deref_ok PERMISSIONS FOR create, update WHERE lookup:[$auth, 'ok'].id != NONE;
DEFINE FIELD w_no ON deref_ok PERMISSIONS FOR create, update WHERE lookup:[$auth, 'no'].id != NONE;
CREATE deref_ok:seed SET secret = 's', hidden = 'h';
DEFINE TABLE deref_no SCHEMALESS PERMISSIONS FOR select, create, update, delete WHERE lookup:[$auth, 'no'].id != NONE;
DEFINE FIELD secret ON deref_no PERMISSIONS FOR select WHERE lookup:[$auth, 'no'].id != NONE;
CREATE deref_no:seed SET secret = 's';
DEFINE TABLE subquery_ok SCHEMALESS PERMISSIONS FOR select, create, update, delete WHERE array::len((SELECT VALUE id FROM lookup WHERE id = lookup:[$auth, 'ok'])) > 0;
DEFINE FIELD secret ON subquery_ok PERMISSIONS FOR select WHERE array::len((SELECT VALUE id FROM lookup WHERE id = lookup:[$auth, 'ok'])) > 0;
DEFINE FIELD hidden ON subquery_ok PERMISSIONS FOR select WHERE array::len((SELECT VALUE id FROM lookup WHERE id = lookup:[$auth, 'no'])) > 0;
DEFINE FIELD w_ok ON subquery_ok PERMISSIONS FOR create, update WHERE array::len((SELECT VALUE id FROM lookup WHERE id = lookup:[$auth, 'ok'])) > 0;
DEFINE FIELD w_no ON subquery_ok PERMISSIONS FOR create, update WHERE array::len((SELECT VALUE id FROM lookup WHERE id = lookup:[$auth, 'no'])) > 0;
CREATE subquery_ok:seed SET secret = 's', hidden = 'h';
DEFINE TABLE subquery_no SCHEMALESS PERMISSIONS FOR select, create, update, delete WHERE array::len((SELECT VALUE id FROM lookup WHERE id = lookup:[$auth, 'no'])) > 0;
DEFINE FIELD secret ON subquery_no PERMISSIONS FOR select WHERE array::len((SELECT VALUE id FROM lookup WHERE id = lookup:[$auth, 'no'])) > 0;
CREATE subquery_no:seed SET secret = 's';
DEFINE TABLE exists_ok SCHEMALESS PERMISSIONS FOR select, create, update, delete WHERE record::exists(lookup:[$auth, 'ok']);
DEFINE FIELD secret ON exists_ok PERMISSIONS FOR select WHERE record::exists(lookup:[$auth, 'ok']);
DEFINE FIELD hidden ON exists_ok PERMISSIONS FOR select WHERE record::exists(lookup:[$auth, 'no']);
DEFINE FIELD w_ok ON exists_ok PERMISSIONS FOR create, update WHERE record::exists(lookup:[$auth, 'ok']);
DEFINE FIELD w_no ON exists_ok PERMISSIONS FOR create, update WHERE record::exists(lookup:[$auth, 'no']);
CREATE exists_ok:seed SET secret = 's', hidden = 'h';
DEFINE TABLE exists_no SCHEMALESS PERMISSIONS FOR select, create, update, delete WHERE record::exists(lookup:[$auth, 'no']);
DEFINE FIELD secret ON exists_no PERMISSIONS FOR select WHERE record::exists(lookup:[$auth, 'no']);
CREATE exists_no:seed SET secret = 's';
DEFINE TABLE graph_ok SCHEMALESS PERMISSIONS FOR select, create, update, delete WHERE gate:ok INSIDE $auth->linked->gate;
DEFINE FIELD secret ON graph_ok PERMISSIONS FOR select WHERE gate:ok INSIDE $auth->linked->gate;
DEFINE FIELD hidden ON graph_ok PERMISSIONS FOR select WHERE gate:no INSIDE $auth->linked->gate;
DEFINE FIELD w_ok ON graph_ok PERMISSIONS FOR create, update WHERE gate:ok INSIDE $auth->linked->gate;
DEFINE FIELD w_no ON graph_ok PERMISSIONS FOR create, update WHERE gate:no INSIDE $auth->linked->gate;
CREATE graph_ok:seed SET secret = 's', hidden = 'h';
DEFINE TABLE graph_no SCHEMALESS PERMISSIONS FOR select, create, update, delete WHERE gate:no INSIDE $auth->linked->gate;
DEFINE FIELD secret ON graph_no PERMISSIONS FOR select WHERE gate:no INSIDE $auth->linked->gate;
CREATE graph_no:seed SET secret = 's';
