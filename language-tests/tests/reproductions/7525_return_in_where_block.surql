/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "RETURN inside a block in a WHERE clause yields the block's value for that row's filter"
issue = 7525

[[test.results]]
value = "[{ a: 2 }]"
*/

SELECT a FROM [{ a: 1 }, { a: 2 }] WHERE { return a > 1 };
