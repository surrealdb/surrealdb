/**
[env]
namespace = true
database = false
clean = true

[test]
reason = """
	A call of a user-defined function needs a database to look the function
	up in. With a namespace selected and no database the statement fails with
	the missing-database error, not with a statement-planning error about the
	function.
"""

[[test.results]]
error = "Specify a database to use"

*/

RETURN fn::nothing();
