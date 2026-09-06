/**
[env]
imports = ["upgrade/define/field_keyword_name_import.surql"]

[test]
upgrade = true
# The upgrade suite hops through adjacent version pairs, and every release below
# 3.4.0 reads a stored keyword field name as an expression — so a hop that ends
# below it, such as 3.0.0 => 3.0.1, would be asserting the schema loads on a
# version that cannot load it. Only a hop that lands on 3.4.0 or later can.
version = ">=3.4.0"
reason = "Issue #7408: a field name that is a reserved keyword is stored bare, so a version that re-reads the stored schema as an expression cannot load the table at all. Reading such a definition back must work, and the table must be writable again, so that a database an older version left unreadable recovers on upgrade."

[[test.results]]
value = "{ events: {  }, fields: { function: 'DEFINE FIELD `function` ON kwfn TYPE string PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "{ events: {  }, fields: { select: 'DEFINE FIELD `select` ON kwsel TYPE string PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "[{ function: 'f', id: kwfn:1 }]"

[[test.results]]
value = "[{ function: 'f', id: kwfn:1 }]"

*/
INFO FOR TABLE kwfn;
INFO FOR TABLE kwsel;
CREATE kwfn:1 SET `function` = 'f';
SELECT * FROM kwfn;
