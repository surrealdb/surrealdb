/**
[env]
namespace = "test"
database = "test"
imports = ["api/permissions/unchecked_frames_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:banned" }

[test]
reason = """
	An API's PERMISSIONS clause is judged with permissions off, so a clause that
	reads a table the invoking record user cannot read sees all of it and
	refuses a banned user; the handler and its middleware run with permissions
	off too, reading every row of a table the invoker may not select, whether
	the handler is a block or an expression.
"""

[[test.results]]
skip-api-request-id = true
value = "{ body: 'Permission denied: You are not allowed to access this resource', headers: {  }, status: 403 }"

[[test.results]]
skip-api-request-id = true
value = "{ body: 'Permission denied: You are not allowed to access this resource', headers: {  }, status: 403 }"

[[test.results]]
skip-api-request-id = true
value = "{ body: [1, 2], headers: {}, status: 200 }"

[[test.results]]
skip-api-request-id = true
value = "{ body: [1, 2], headers: {}, status: 200 }"

[[test.results]]
skip-api-request-id = true
value = "{ body: 'ok', headers: { 'x-secret': '[1, 2]' }, status: 200 }"

[[test.results]]
value = "[]"
*/

api::invoke("/members");
api::invoke("/members_fn");
api::invoke("/secret");
api::invoke("/secret_block");
api::invoke("/secret_middleware");
SELECT * FROM secret;
