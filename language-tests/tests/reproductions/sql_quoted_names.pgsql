/**
[env]
namespace = true
database = true
imports = ["reproductions/sql_quoted_names_import.surql"]

[test]
reason = "Quoted table and column names retain their case in SQL reads, writes and RETURNING."

[[test.results]]
value = "[{ id: 1, ID: 99, createdAt: 'upper', createdat: 'lower' }]"
[[test.results]]
value = "[{ createdAt: 'updated' }]"
[[test.results]]
value = "[{ ID: 99 }]"
*/
SELECT id, "ID", "createdAt", CREATEDAT FROM "User";
UPDATE "User" SET "createdAt" = 'updated' WHERE "ID" = 99 RETURNING "createdAt";
DELETE FROM "User" WHERE id = 1 RETURNING "ID";
