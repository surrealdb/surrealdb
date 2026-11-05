/**
[test]
reason = """
	An event whose body runs `UPDATE` on a record that does not exist does not
	create it: `UPDATE` changes only records that exist. Here the event fires
	while `User:u1` is still being created by the statement around it, so the
	event's `UPDATE` changes nothing and the outer `CREATE` then stores
	`User:u1` without conflict, with no `user-accounts`. Once the record exists,
	the same event does change it.
"""
issue = 4382

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ I_accounts: UserAccounts:ua1, id: User:u1 }"

[[test.results]]
value = "[{ I_accounts: UserAccounts:ua1, id: User:u1 }]"

[[test.results]]
value = "[{ id: User:u2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = '[{ I_accounts: UserAccounts:ua1, id: User:u1 }, { id: User:u2, "user-accounts": [UserAccounts:ua1] }]'

[env]
namespace = true
database = true
auth = { level = "owner" }
*/
DEFINE TABLE UserAccounts SCHEMAFULL PERMISSIONS FULL;
DEFINE FIELD user ON TABLE UserAccounts TYPE record<User | God | SuperUser>;
DEFINE EVENT update_user ON TABLE UserAccounts WHEN $before.user != $after.user THEN {
	UPDATE $after.user SET `user-accounts` = [$after.id];
};
CREATE ONLY User:u1 SET I_accounts = (CREATE ONLY UserAccounts:ua1 SET user = User:u1 RETURN id AS sid).sid;
SELECT * FROM User;
CREATE User:u2;
UPDATE UserAccounts:ua1 SET user = User:u2 RETURN NONE;
SELECT * FROM User ORDER BY id;
