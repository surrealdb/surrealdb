/**
[env]
namespace = true
database = true

[test]
reason = "A function that has its own lowering is refused as unsupported in the expressions a write lowers, which call the engine function the registry names"

[test.results]
request-error = "PostgreSQL DELETE lowering error: unsupported expression: date_part in a write statement"
*/

DELETE FROM wr WHERE date_part('year', at) < 2020;
