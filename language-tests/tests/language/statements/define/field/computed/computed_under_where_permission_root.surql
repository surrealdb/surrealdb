/**
[env]
imports = ["language/statements/define/field/computed/computed_under_where_permission_import.surql"]

[test]
reason = """
	The root principal sees every field, so a read derives every computed
	field and each body runs. The first field calling `fn::r` is derived like
	the others, and its body, which writes through `fn::r`, is refused by the
	COMPUTED frame. `gated` throws through its stored function when read, since
	nothing hides it from root. Hidden computed fields are not evaluated for a
	reader who cannot see them, which computed_under_where_permission pins; here
	the same bodies run because the permission admits.
"""

[[test.results]]
error = "The COMPUTED clause must not have effects: `CREATE` writes data"

[[test.results]]
error = "An error occurred: boom"
*/

SELECT * FROM doc;
SELECT * FROM gate;
