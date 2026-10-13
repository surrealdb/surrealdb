/**
[env]
namespace = true
database = true

[test]
reason = "Register #28, row FieldValueFrame: a schema statement is not allowed in the VALUE clause of a field, whoever defines the field. The request is refused at plan time, before any statement of it runs."

[test.results]
request-error = "Invalid statement: a schema statement is not allowed in a VALUE clause (statement 1)"
*/
DEFINE FIELD h ON u VALUE (DEFINE USER escalated ON DATABASE ROLES OWNER PASSWORD 'secret');
