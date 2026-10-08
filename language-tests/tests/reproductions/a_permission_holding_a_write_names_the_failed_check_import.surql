/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Setup for a_permission_holding_a_write_names_the_failed_check.surql."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: d:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ f: 1, id: g:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: h:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: r1:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: r3:1 }]"
*/
DEFINE TABLE d SCHEMALESS PERMISSIONS FOR select WHERE (CREATE x:1) != NONE, FOR create, update FULL;
CREATE d:1;
DEFINE TABLE g SCHEMALESS PERMISSIONS FULL;
DEFINE FIELD f ON g TYPE option<int> PERMISSIONS FOR select WHERE (DELETE x) != NONE;
CREATE g:1 SET f = 1;
DEFINE FUNCTION fn::w() { CREATE x:2; RETURN true };
DEFINE TABLE h SCHEMALESS PERMISSIONS FOR select WHERE fn::w(), FOR create FULL;
CREATE h:1;
DEFINE TABLE r1 SCHEMALESS PERMISSIONS FOR select WHERE { THROW "boom" }, FOR create FULL;
CREATE r1:1;
DEFINE TABLE r3 SCHEMALESS PERMISSIONS FOR select WHERE math::sqrt("a") > 0, FOR create FULL;
CREATE r3:1;
