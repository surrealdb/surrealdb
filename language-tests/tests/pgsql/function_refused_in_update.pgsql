/**
[env]
namespace = true
database = true

[test]
reason = "A function that has its own lowering is refused as unsupported in the expressions a write lowers, which call the engine function the registry names"

[test.results]
request-error = "PostgreSQL UPDATE lowering error: unsupported expression: substring in a write statement"
*/

UPDATE wr SET s = substring(s, 1, 2) WHERE id = 1;
