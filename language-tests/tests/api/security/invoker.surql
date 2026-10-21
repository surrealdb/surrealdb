/**
[env]
namespace = "test"
database = "test"
imports = ["api/security/invoker_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:test" }

[test]
issue = 6734
reason = "A DEFINE API handler runs with table and field permissions lifted unless the API says SECURITY INVOKER, in which case its queries are judged exactly as the caller's own would be. The first result is what the caller gets from a direct query; every INVOKER read must equal it, and every DEFINER read must see the hidden row and the secret field. Checked for the handler, the fallback, middleware, an ALTERed mode, and APIs called from a handler."

[[test.results]]
value = "[{ id: category:shown, name: 'shown', visible: true }]"

[[test.results]]
value = "[{ id: category:hidden, name: 'hidden', secret: 's2', visible: false }, { id: category:shown, name: 'shown', secret: 's1', visible: true }]"

[[test.results]]
value = "[{ id: category:hidden, name: 'hidden', secret: 's2', visible: false }, { id: category:shown, name: 'shown', secret: 's1', visible: true }]"

[[test.results]]
value = "[{ id: category:shown, name: 'shown', visible: true }]"

[[test.results]]
value = "[{ id: category:shown, name: 'shown', visible: true }]"

[[test.results]]
value = "[{ id: category:shown, name: 'shown', visible: true }]"

[[test.results]]
value = "[{ id: category:shown, name: 'shown', visible: true }]"

[[test.results]]
value = "[{ id: category:shown, name: 'shown', visible: true }]"

[[test.results]]
value = "[{ id: category:hidden, name: 'hidden', secret: 's2', visible: false }, { id: category:shown, name: 'shown', secret: 's1', visible: true }]"

[[test.results]]
value = "[{ id: category:shown, name: 'shown', visible: true }]"

[[test.results]]
value = "[{ id: category:hidden, name: 'hidden', secret: 's2', visible: false }, { id: category:shown, name: 'shown', secret: 's1', visible: true }]"

[[test.results]]
value = "[{ id: category:hidden, name: 'hidden', secret: 's2', visible: false }, { id: category:shown, name: 'shown', secret: 's1', visible: true }]"

[[test.results]]
value = "[{ id: category:shown, name: 'shown', visible: true }]"
*/

// 0 - What the caller gets from a direct query.
SELECT * FROM category ORDER BY id;

// 1, 2 - Without the clause, and with SECURITY DEFINER spelled out, the handler
// sees everything.
(api::invoke("/default/read")).body;
(api::invoke("/definer/read")).body;

// 3 - SECURITY INVOKER: the caller's view.
(api::invoke("/invoker/read")).body;

// 4 - The fallback handler of an INVOKER API.
(api::invoke("/invoker/fallback", { method: "post" })).body;

// 5 - Trailing clauses in the other order.
(api::invoke("/invoker/comment_first")).body;

// 6 - Middleware in front of an INVOKER handler.
(api::invoke("/invoker/middleware")).body;

// 7, 8, 9 - ALTER API ... SECURITY INVOKER, ALTER API ... SECURITY DEFINER, and an
// ALTER that names neither.
(api::invoke("/altered/to_invoker")).body;
(api::invoke("/altered/to_definer")).body;
(api::invoke("/altered/comment_only")).body;

// 10 - An INVOKER handler called from a DEFINER handler is called by a handler
// that is not subject to permissions, so it is not either. This is the same
// answer a SECURITY INVOKER function called from a SECURITY DEFINER one gives.
(api::invoke("/nested/definer_calls_invoker")).body;

// 11 - A DEFINER API called from an INVOKER handler still lifts permissions: the
// mode belongs to the API being run.
(api::invoke("/nested/invoker_calls_definer")).body;

// 12 - INVOKER calling INVOKER stays the caller's view.
(api::invoke("/nested/invoker_calls_invoker")).body;
