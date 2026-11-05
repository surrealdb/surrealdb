/**
[env]
namespace = true
database = true

[test]
reason = """
	By design (issue 4470). `tag:a` is created inside the value of the CREATE of
	`user:1`, so its event runs before that outer CREATE has stored the record.
	`UPDATE` of a record that does not exist is a no-op, so the event's update of
	`user:1` writes nothing and the outer CREATE then stores `user:1` without
	`tags`. An event that creates the record it points at makes the outer CREATE
	fail, because a CREATE never overwrites a record. An `UPSERT` that meets the
	record its own value created through an event fails the same way: the
	statement's attempt is rolled back with the event's write, so there is no
	record left to update, and the statement reports the conflict rather than
	succeeding with no effect.
"""
issue = 4470

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: user:1, nested: { id: tag:a, users: [user:1] } }]"

[[test.results]]
value = "[{ id: user:1, tags: NONE }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Database record `user:2` already exists"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Database record `user:3` already exists"

[[test.results]]
value = "[{ id: user:1 }]"
*/
DEFINE EVENT link ON TABLE tag WHEN $before.users != $after.users THEN {
	FOR $u IN $after.users { UPDATE $u SET tags += [$after.id]; };
};
CREATE user:1 SET nested = (CREATE ONLY tag:a SET users = [user:1]);
SELECT id, tags FROM user;
DEFINE EVENT link_create ON TABLE tag2 WHEN $before.users != $after.users THEN {
	FOR $u IN $after.users { CREATE $u SET tags = [$after.id]; };
};
CREATE user:2 SET nested = (CREATE ONLY tag2:a SET users = [user:2]);
DEFINE EVENT link_upsert ON TABLE tag3 WHEN $before.users != $after.users THEN {
	FOR $u IN $after.users { UPSERT $u SET tags += [$after.id]; };
};
UPSERT user:3 SET nested = (UPSERT ONLY tag3:a SET users = [user:3]);
SELECT id FROM user;
