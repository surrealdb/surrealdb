/**
[env]
namespace = "test"
database = "test"
imports = ["api/security/invoker_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:test" }

[test]
issue = 6734
reason = "Under SECURITY INVOKER a handler's writes are refused by the caller's table permissions, in the same way the same statements are when the caller runs them directly: each returns no record and the table is left as it was. Without the clause the handler's write goes through and returns the record, secret field included."

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: category:shown, name: 'shown', visible: true }]"

[[test.results]]
value = "[{ id: category:injected, name: 'injected', secret: 's3', visible: true }]"

[[test.results]]
value = "[{ id: category:injected, name: 'injected', visible: true }, { id: category:shown, name: 'shown', visible: true }]"
*/

// 0, 1, 2 - What the caller gets from the same writes run directly.
CREATE category:direct SET name = "direct", visible = true, secret = "s";
UPDATE category:shown SET name = "renamed";
DELETE category:shown RETURN BEFORE;

// 3, 4, 5 - The same writes from an INVOKER handler.
(api::invoke("/invoker/write", { method: "post" })).body;
(api::invoke("/invoker/update", { method: "post" })).body;
(api::invoke("/invoker/delete", { method: "post" })).body;

// 6 - Nothing was created, renamed or deleted.
SELECT * FROM category ORDER BY id;

// 7 - A handler without the clause writes freely and sees the whole record.
(api::invoke("/default/write", { method: "post" })).body;

// 8 - That write is visible afterwards, minus the field the caller may not read.
SELECT * FROM category ORDER BY id;
