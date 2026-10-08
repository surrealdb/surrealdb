/**
[test]
reason = """A record-id parameter whose key holds a closure has no literal form; a \
SELECT over it keeps the parameter as its source and evaluates it at runtime instead \
of resolving it against the catalog at bind time, where the key fails to serialize."""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Tried to serialize a value which cannot be serialized."

[[test.results]]
error = "Tried to serialize a value which cannot be serialized."
*/

DEFINE TABLE t SCHEMALESS;

LET $r = t:{ f: || 1 };

RETURN (SELECT * FROM $r).len();

SELECT * FROM $r;
