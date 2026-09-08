/**
[test]
# The from-side of the hop must still write the legacy `*sq` layout, or the
# migration has nothing to move and the test passes while covering nothing.
version = "<3.3.0"
reason = "Writes, on the from-version, a sequence definition in the legacy `*sq` table band alongside a table whose name begins with `sq`, which is the state the 3.3 migration has to untangle"

[[test.results]]
value = "NONE"

[[test.results]]
value = "0"

[[test.results]]
value = "1"

[[test.results]]
value = "2"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, id: sqfoo:1 }]"

*/

-- On the from-version this definition lands at /*{ns}*{db}*sq{name}.
DEFINE SEQUENCE foo BATCH 50 START 0;
sequence::nextval('foo');
sequence::nextval('foo');
sequence::nextval('foo');

-- `sqfoo`'s keyspace root is byte-identical to sequence `foo`'s definition key,
-- so its keys share the prefix the definition scan covers. The migration must
-- move the definition and leave every one of these alone.
DEFINE TABLE sqfoo SCHEMAFULL;
DEFINE FIELD a ON sqfoo TYPE int;
CREATE sqfoo:1 SET a = 1;
