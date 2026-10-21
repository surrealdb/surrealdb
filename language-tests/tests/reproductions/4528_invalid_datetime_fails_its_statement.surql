/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A value that is no datetime fails the statement that reads it, and the session
goes on. Over the RPC protocol an invalid datetime in a request's parameters
is answered as an error to that request's id, with the connection kept, which
`the_id_survives_a_datetime_that_is_not_one` in surrealdb-core checks.
"""
issue = 4528

[[test.results]]
error = "Could not cast into `datetime` using input `'ffff'`"

[[test.results]]
value = "d'2024-01-02T00:00:00Z'"
*/
RETURN <datetime> 'ffff';
RETURN d'2024-01-01T00:00:00Z' + 1d;
