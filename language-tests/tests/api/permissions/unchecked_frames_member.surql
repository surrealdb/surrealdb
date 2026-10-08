/**
[env]
namespace = "test"
database = "test"
imports = ["api/permissions/unchecked_frames_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:member" }

[test]
reason = """
	An API's PERMISSIONS clause that reads a table the invoking record user
	cannot read admits a user the table does not name.
"""

[[test.results]]
skip-api-request-id = true
value = "{ body: 'in', headers: {}, status: 200 }"

[[test.results]]
skip-api-request-id = true
value = "{ body: 'in', headers: {}, status: 200 }"
*/

api::invoke("/members");
api::invoke("/members_fn");
