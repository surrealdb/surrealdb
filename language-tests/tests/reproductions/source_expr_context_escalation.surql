/**
[env]
namespace = false
database = false
auth = { level = "owner" }

[test]
reason = "SourceExpr and SelectProject should not require Database context for pure value-source queries"

[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
value = "[{ field: 1 }, { field: 2 }]"

[[test.results]]
value = "[{ a: 1, b: 2 }, { a: 3, b: 4 }]"

[[test.results]]
value = "[{ b: 2 }, { b: 4 }]"

*/

-- Pure value arrays should not need a database
SELECT * FROM [1, 2, 3];
SELECT field FROM [{ field: 1 }, { field: 2 }];
SELECT * FROM [{ a: 1, b: 2 }, { a: 3, b: 4 }];
SELECT b FROM [{ a: 1, b: 2 }, { a: 3, b: 4 }];
