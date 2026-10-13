/**
# Record-reference dataset: 5000 `user` records and 15000 `comment` records whose
# `author` is a REFERENCE to a random user, so reverse-reference (`<~`) traversals
# have back-links to resolve. Each user also has a `friends` array of record links
# (~3 each) for the array-FETCH bench. Statements are top-level (not a `{ }` block)
# so the REFERENCE field is committed before the comments populate it. `run = false`.

[test]
run = false
*/

{
	DEFINE TABLE user SCHEMALESS;

	DEFINE TABLE comment SCHEMALESS;

	DEFINE FIELD author ON comment TYPE record<user> REFERENCE;

	DEFINE FIELD friends ON user TYPE array<record<user>>;

	CREATE |user:1..=5000| SET
		name = rand::string(12),
		friends = [
			type::record('user', rand::int(1, 5000)),
			type::record('user', rand::int(1, 5000)),
			type::record('user', rand::int(1, 5000))
		]
	RETURN NONE;

	CREATE |comment:1..15000| SET
		author = type::record('user', rand::int(1, 5000)),
		text = rand::string(50)
	RETURN NONE;

	RETURN 'OK';
}
