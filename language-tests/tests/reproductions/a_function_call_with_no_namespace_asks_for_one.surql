/**
[env]
namespace = false
database = false
clean = true

[test]
reason = """
	A call of a user-defined function needs a namespace and a database to
	look the function up in. With none selected the statement fails with the
	missing-selection error, as every other statement that needs one does,
	not with a statement-planning error about the function.
"""

[[test.results]]
error = "Specify a namespace to use"

[[test.results]]
error = "Specify a namespace to use"

*/

RETURN fn::nothing();
SELECT VALUE fn::nothing() FROM [1];
