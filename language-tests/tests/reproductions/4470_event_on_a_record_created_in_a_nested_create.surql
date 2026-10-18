/**
[env]
namespace = true
database = true

[test]
reason = """
	Known defect, pinned as it behaves today (issue 4470). `tag:a` is created
	inside the value of the CREATE of `user:1`, and its event updates `user:1`
	before that outer CREATE has stored the record. The outer CREATE then stores
	its own image over the event's write, so `tags` is lost: the expected final
	value is `[{ id: user:1, tags: [tag:a] }]`. When this is fixed, update the
	last expectation.
"""
issue = 4470

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: user:1, nested: { id: tag:a, users: [user:1] } }]"

[[test.results]]
value = "[{ id: user:1, tags: NONE }]"
*/
DEFINE EVENT link ON TABLE tag WHEN $before.users != $after.users THEN {
	FOR $u IN $after.users { UPDATE $u SET tags += [$after.id]; };
};
CREATE user:1 SET nested = (CREATE ONLY tag:a SET users = [user:1]);
SELECT id, tags FROM user;
