/**
[env]
imports = ["reproductions/6389_computed_field_rederived_after_record_user_update_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = "A COMPUTED field (which replaced `<future>`) is derived from the record as it stands after the write, for a record user as for the owner: the image an UPDATE returns, and a path read off it, see the field the UPDATE just set."
issue = 6389

[[test.results]]
value = "{ result: NONE }"

[[test.results]]
value = "1"

[[test.results]]
value = "{ result: NONE }"

[[test.results]]
value = "{ id: proxy:1, result: 1, run: true }"

[[test.results]]
value = "1"
*/

CREATE ONLY proxy:1 RETURN result;
(UPDATE ONLY proxy:1 SET run = true).result;
UPDATE ONLY proxy:1 SET run = false RETURN result;
UPDATE ONLY proxy:1 SET run = true RETURN AFTER;
proxy:1.result;
