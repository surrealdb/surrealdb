/**
[env]
namespace = true
database = true
imports = ["access/record/permission_predicate_lookups_and_destructures_import.surql"]
signin = """{ ns: "test", db: "test", ac: "account", user: "alice" }"""

[test]
reason = "A select permission holding a filtered graph lookup or a destructure, and computed fields holding a method and an element filter, are judged for a record user as they are written; the write that plants them as the owner runs too"

[[test.results]]
value = "[doc:1]"

[[test.results]]
value = "[pd:1]"

[[test.results]]
value = "[{ id: cu:1, m: [2, 3], n: 3, tags: [1, 2, 3] }]"

[[test.results]]
value = "[{ m: [2, 3], n: 3 }]"
*/
SELECT VALUE id FROM doc;
SELECT VALUE id FROM pd;
SELECT * FROM cu;
SELECT n, m FROM cu;
