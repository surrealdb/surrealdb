/**
[env]
namespace = true
database = true

[test]
reason = "The namespace and database arguments of USE are expressions, so evaluating one can read or write the datastore. The statement's transaction must be installed on the context before they are computed: `Context::tx()` has no fallback for a context without a transaction, so an argument that reaches the datastore has to surface as a query error rather than a panic. Release builds are `panic = 'abort'`, which made this an abort of the whole server process."
issue = 903

[[test.results]]
value = "NONE"

# 2: An argument that reads the datastore evaluates against the statement's
# transaction. The empty table yields an empty array, which is not a name.
[[test.results]]
error = "Expected `database` but found `[]`"

# 3: A statement argument is evaluated the same way, for either position.
[[test.results]]
error = "Expected `database` but found `{  }`"

[[test.results]]
error = "Expected `namespace` but found `{  }`"

# 5: An argument that writes returns NONE, which is not a name. The failed
# statement rolls its transaction back, so the namespace is not created.
[[test.results]]
error = "Expected `namespace` but found `NONE`"

# 6: None of the rejected statements changed the session, and it still works.
[[test.results]]
value = "[]"

# 7: A computed name that does coerce to a string selects that database.
[[test.results]]
value = "{ database: 'x', namespace: 'test' }"

# 8: Parameters are the ordinary way to compute a name.
[[test.results]]
value = "NONE"

[[test.results]]
value = "{ database: 'test', namespace: 'test' }"

[[test.results]]
value = "[]"
*/

DEFINE TABLE OVERWRITE anything;
USE DB (SELECT VALUE id FROM anything);
USE DB (INFO FOR DB).analyzers;
USE NS (INFO FOR DB).analyzers;
USE NS DEFINE NAMESPACE zz;
SELECT * FROM anything;
USE DB type::string("x");
LET $db = "test";
USE DB $db;
SELECT * FROM anything;
