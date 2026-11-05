/**
[env]
namespace = true
database = true

[test]
reason = """
A field's `VALUE`, `DEFAULT` and `ASSERT` clauses, and other computed fields, read
a `COMPUTED` field by the name the record would hold it by, and the write has to
derive the computed field before the clause that reads it. A name that has to be
backticked when written is a dependency like any other: the clauses below read `z last`,
which sorts after the names reading it, so a dependency that is not found leaves each
of them reading a field that was never derived. `own n` reads its own field, which
is the value under consideration rather than a dependency.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """[{ "a first": 'q+0+1', copied: 'q+0', "copied 2": 'q+0+1', dflt: 'q+0', id: d:1, "own n": 'init', src: 'q', "z last": 'q+0' }]"""

[[test.results]]
value = """[{ "a first": 'r+0+1', copied: 'r+0', "copied 2": 'r+0+1', dflt: 'q+0', id: d:1, "own n": 'init', src: 'r', "z last": 'r+0' }]"""

[[test.results]]
value = """[{ "a first": 'q+0+1', checked: 'q+0', copied: 'q+0', "copied 2": 'q+0+1', dflt: 'q+0', id: d:2, "own n": 'init', src: 'q', "z last": 'q+0' }]"""

[[test.results]]
error = "Found 'nope' for field `checked`, with record `d:3`, but field must conform to: $value = NONE OR $value = `z last`"

[[test.results]]
value = """[{ "a first": 'r+0+1', copied: 'r+0', "copied 2": 'r+0+1', dflt: 'q+0', id: d:1, "own n": 'init', src: 'r', "z last": 'r+0' }, { "a first": 'q+0+1', checked: 'q+0', copied: 'q+0', "copied 2": 'q+0+1', dflt: 'q+0', id: d:2, "own n": 'init', src: 'q', "z last": 'q+0' }]"""
*/

DEFINE TABLE d SCHEMALESS;
DEFINE FIELD src ON d TYPE option<string>;
DEFINE FIELD `a first` ON d COMPUTED string::concat(`z last`, '+1');
DEFINE FIELD `z last`  ON d COMPUTED string::concat(src, '+0');
DEFINE FIELD copied    ON d VALUE `z last`;
DEFINE FIELD `copied 2` ON d VALUE `a first`;
DEFINE FIELD dflt      ON d DEFAULT `z last`;
DEFINE FIELD checked   ON d TYPE option<string> ASSERT $value = NONE OR $value = `z last`;
DEFINE FIELD `own n`   ON d VALUE `own n` ?? 'init';

-- Each clause sees the derived value, the computed chain is derived in order.
CREATE d:1 SET src = 'q' RETURN AFTER;
UPDATE d:1 SET src = 'r' RETURN AFTER;

-- An ASSERT that reads a computed field is judged against its derived value.
CREATE d:2 SET src = 'q', checked = 'q+0' RETURN AFTER;
CREATE d:3 SET src = 'q', checked = 'nope';
SELECT * FROM d ORDER BY id;
