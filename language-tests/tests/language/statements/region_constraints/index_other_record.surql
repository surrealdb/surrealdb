/**
[env]
namespace = true
database = true

[test]
reason = "Register #28, row IndexColumn: a read of another record is not allowed in the column of an index, which may depend on the indexed record alone. The request is refused at plan time, before any statement of it runs."

[test.results]
request-error = "Invalid statement: a read of another record is not allowed in an index definition (statement 1)"
*/
DEFINE INDEX i ON q FIELDS (SELECT VALUE v FROM other:1);
