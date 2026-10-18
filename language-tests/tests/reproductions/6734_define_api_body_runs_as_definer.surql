/**
[env]
imports = ["reproductions/6734_define_api_body_runs_as_definer_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:bob" }

[test]
reason = "Design: a DEFINE API handler runs with permissions off under the definer's authority, as an event does, so it reads a table the invoking record user cannot select directly. A caller-permission mode is a feature (SECURITY INVOKER) not yet on this tree."
issue = 6734

[[test.results]]
skip-api-request-id = true
value = "{ body: [42], headers: {}, status: 200 }"

[[test.results]]
value = "[]"
*/

api::invoke("/secret");
SELECT * FROM secret;
