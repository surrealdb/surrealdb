/**
[env]
namespace = true
database = true

[test]
reason = "INFO ... STRUCTURE reports a statement-shaped clause parenthesised, so the text it hands back can be spliced after the clause keyword and read as one expression. Pinned because it is a client-visible output change and nothing else covers it."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'(SELECT VALUE x FROM y)'"

[[test.results]]
value = "['(CREATE y)']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'1 + 1'"
*/

DEFINE TABLE y;
DEFINE TABLE t SCHEMALESS;
DEFINE FIELD f ON t VALUE (SELECT VALUE x FROM y);
DEFINE EVENT e ON t WHEN true THEN (CREATE y);

-- Statement-shaped clauses keep their parentheses...
RETURN (INFO FOR TABLE t STRUCTURE).fields[0].value;
RETURN (INFO FOR TABLE t STRUCTURE).events[0].then;

-- ...and value-shaped ones gain none.
DEFINE FIELD g ON t VALUE 1 + 1;
RETURN (INFO FOR TABLE t STRUCTURE).fields[1].value;
