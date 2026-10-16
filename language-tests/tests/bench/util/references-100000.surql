/**
# Record-reference dataset at scale: 20000 `user` records and 100000 `comment` records whose
# `author` is a REFERENCE to a random user. Each user holds a `friends` array of 3 record links and
# a `tags` array of 20 record links into `user`, for the array-FETCH and the link-dereference
# benches. Statements are in a `{ }` block committed before the benches run. `run = false`.

[test]
run = false
*/

{
	DEFINE TABLE user SCHEMALESS;

	DEFINE TABLE comment SCHEMALESS;

	DEFINE FIELD author ON comment TYPE record<user> REFERENCE;

	DEFINE FIELD friends ON user TYPE array<record<user>>;

	CREATE |user:1..=20000| SET
		name = rand::string(12),
		friends = [
			type::record('user', rand::int(1, 20000)),
			type::record('user', rand::int(1, 20000)),
			type::record('user', rand::int(1, 20000))
		],
		tags = array::map(<array>0..20, |$i| type::record('user', rand::int(1, 20000)))
	RETURN NONE;

	CREATE |comment:1..=100000| SET
		author = type::record('user', rand::int(1, 20000)),
		text = rand::string(50)
	RETURN NONE;

	RETURN 'OK';
}
