/**
[env]
namespace = true
database = true
imports = ["pgsql/_declared_columns_perms.surql"]
auth = { level = "viewer" }

[test]
reason = "A system user that may view the definitions of tables reads the declared columns, a field its permissions hide among them, as the datastore answers them"

[[test.results]]
value = "[{ hidden: 'h', id: 1, name: 'Ann', nick: 'A' }, { hidden: NONE, id: 2, name: 'Ben', nick: NONE }]"
*/

SELECT * FROM declcol_acct ORDER BY id;
