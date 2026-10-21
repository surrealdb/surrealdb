/**
[env]
imports = ["reproductions/7464_auth_id_when_the_session_cannot_read_its_record_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:ghost" }

[test]
reason = "`$auth.id` is answered from the session, so it names the signed-in record even once that record no longer exists; `record::exists($auth)` is the check for whether it does."
issue = 7464

[[test.results]]
value = "person:ghost"

[[test.results]]
value = "NONE"

[[test.results]]
value = "false"
*/

RETURN $auth.id;
RETURN $auth.name;
RETURN record::exists($auth);
