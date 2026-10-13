/**
[env]
namespace = true
database = true

[test]
reason = "Register #28, row FieldValueFrame: a write is not allowed in the VALUE clause of a field. The request is refused at plan time, before any statement of it runs."

[test.results]
request-error = "Invalid statement: a write is not allowed in a VALUE clause (statement 1)"
*/
DEFINE FIELD f ON v VALUE (CREATE log2:1 RETURN NONE);
