/**
[env]
namespace = true
database = true
imports = ["pgsql/_join_lookup_errors.surql"]

[test]
reason = "A writing statement over a second table through its FROM or USING clause is refused rather than lowered with the second table as one more target, so it never reads that table whole: the fallible computed field of the table cannot raise for a row that no key reaches, and the table of the statement is not written."

[test.results]
request-error = "PostgreSQL UPDATE lowering error: unsupported statement: UPDATE FROM requires join-based write lowering"
*/

UPDATE f SET n = 1 FROM e WHERE f.k = e.k;
