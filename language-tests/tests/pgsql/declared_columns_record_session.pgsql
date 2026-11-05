/**
[env]
namespace = true
database = true
imports = ["pgsql/_declared_columns_perms.surql"]
auth = { namespace = "test", database = "test", access = "test", rid = "users:alice" }

[test]
reason = "A record user is told nothing of the columns a table declares: a name it does not have reads as NONE rather than failing, which would tell a column that is declared from one that is not, and * is the rows' own fields rather than the declared columns"

[[test.results]]
value = "[{ name: 'Ann', nosuch: NONE }, { name: 'Ben', nosuch: NONE }]"

[[test.results]]
value = "[{ hidden: NONE, name: 'Ann', nick: 'A' }, { hidden: NONE, name: 'Ben', nick: NONE }]"

[[test.results]]
value = "[{ id: 1, name: 'Ann', nick: 'A' }, { id: 2, name: 'Ben' }]"
*/

SELECT name, nosuch FROM declcol_acct ORDER BY id;

SELECT name, hidden, nick FROM declcol_acct ORDER BY id;

SELECT * FROM declcol_acct ORDER BY id;
