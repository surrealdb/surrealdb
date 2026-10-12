/**
[env]
namespace = true
database = true

[test]
reason = """
A foreign table (a view) is recomputed from the record after a write, and a
`COMPUTED` field is never stored, so the view has to be handed it derived under the
name the record would hold it by. A name that has to be backticked when written
groups, and projects, like any other: the view holds the derived value rather than
`NONE`, and the group follows the record as the field's inputs change.

`plain` is the control. The table carries an event, so its writes are observed and
derive the computed fields onto the record the views are recomputed from.
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
value = "NONE"

[[test.results]]
value = """[{ "a-b": 'q!', id: b:1, plain: 'q', src: 'q', "the x": 'q' }]"""

[[test.results]]
value = """[{ "a-b": 'q!', id: b:2, plain: 'q', src: 'q', "the x": 'q' }]"""

[[test.results]]
value = """[{ "a-b": 'r!', id: b:3, plain: 'r', src: 'r', "the x": 'r' }]"""

[[test.results]]
value = "[{ id: by_plain:['q'], n: 2, plain: 'q' }, { id: by_plain:['r'], n: 1, plain: 'r' }]"

[[test.results]]
value = """[{ id: by_space:['q'], n: 2, "the x": 'q' }, { id: by_space:['r'], n: 1, "the x": 'r' }]"""

[[test.results]]
value = """[{ "a-b": 'q!', id: by_hyphen:['q!'], n: 2 }, { "a-b": 'r!', id: by_hyphen:['r!'], n: 1 }]"""

[[test.results]]
value = "[{ id: rows_space:1, src: 'q', tx: 'q' }, { id: rows_space:2, src: 'q', tx: 'q' }, { id: rows_space:3, src: 'r', tx: 'r' }]"

[[test.results]]
value = """[{ "a-b": 'q!', id: b:3, plain: 'q', src: 'q', "the x": 'q' }]"""

[[test.results]]
value = "[]"

[[test.results]]
value = """[{ id: by_space:['q'], n: 2, "the x": 'q' }]"""

[[test.results]]
value = "[{ id: rows_space:2, src: 'q', tx: 'q' }, { id: rows_space:3, src: 'q', tx: 'q' }]"
*/

DEFINE TABLE b SCHEMALESS;
DEFINE FIELD src ON b TYPE option<string>;
DEFINE FIELD plain   ON b COMPUTED src;
DEFINE FIELD `the x` ON b COMPUTED src;
DEFINE FIELD `a-b`   ON b COMPUTED string::concat(src, '!');
DEFINE EVENT observed ON b WHEN false THEN {};

DEFINE TABLE by_plain AS SELECT count() AS n, plain FROM b GROUP BY plain;
DEFINE TABLE by_space AS SELECT count() AS n, `the x` FROM b GROUP BY `the x`;
DEFINE TABLE by_hyphen AS SELECT count() AS n, `a-b` FROM b GROUP BY `a-b`;
DEFINE TABLE rows_space AS SELECT `the x` AS tx, src FROM b;

CREATE b:1 SET src = 'q';
CREATE b:2 SET src = 'q';
CREATE b:3 SET src = 'r';
SELECT * FROM by_plain ORDER BY plain;
SELECT * FROM by_space ORDER BY `the x`;
SELECT * FROM by_hyphen ORDER BY `a-b`;
SELECT * FROM rows_space ORDER BY id;

-- Moving a record between groups moves its count; a delete takes it out.
UPDATE b:3 SET src = 'q';
DELETE b:1;
SELECT * FROM by_space ORDER BY `the x`;
SELECT * FROM rows_space ORDER BY id;
