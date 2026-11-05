/**
[env]
namespace = true
database = true

[test]
reason = """
An event is handed both images of a write, and a `COMPUTED` field is on neither
until the write derives it, under the key the record would hold it by. A name that
has to be backticked when written must be derived under the name itself: the event's
`WHEN` reads it bare, and its body reads it from `$before` and `$after`. `plain` is
the control. The same event runs for a create, an update and a delete, so each
side of each image is pinned: a create has no earlier version, a delete no later
one.
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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """[{ "1st": 'q/digit', "a-b": 'q/hyphen', "café": 'q/unicode', id: b:1, plain: 'q/plain', src: 'q', "the x": 'q/space' }]"""

[[test.results]]
value = """[{ "1st": 'r/digit', "a-b": 'r/hyphen', "café": 'r/unicode', id: b:1, plain: 'r/plain', src: 'r', "the x": 'r/space' }]"""

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ ev: 'CREATE', now: 'q/digit', was: NONE, which: '1st' }, { ev: 'DELETE', now: NONE, was: 'r/digit', which: '1st' }, { ev: 'UPDATE', now: 'r/digit', was: 'q/digit', which: '1st' }, { ev: 'CREATE', now: 'q/hyphen', was: NONE, which: 'a-b' }, { ev: 'DELETE', now: NONE, was: 'r/hyphen', which: 'a-b' }, { ev: 'UPDATE', now: 'r/hyphen', was: 'q/hyphen', which: 'a-b' }, { ev: 'CREATE', now: 'q/unicode', was: NONE, which: 'café' }, { ev: 'DELETE', now: NONE, was: 'r/unicode', which: 'café' }, { ev: 'UPDATE', now: 'r/unicode', was: 'q/unicode', which: 'café' }, { ev: 'CREATE', now: 'q/plain', was: NONE, which: 'plain' }, { ev: 'DELETE', now: NONE, was: 'r/plain', which: 'plain' }, { ev: 'UPDATE', now: 'r/plain', was: 'q/plain', which: 'plain' }, { ev: 'CREATE', now: 'q/space', was: NONE, which: 'the x' }, { ev: 'DELETE', now: NONE, was: 'r/space', which: 'the x' }, { ev: 'UPDATE', now: 'r/space', was: 'q/space', which: 'the x' }]"

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
value = """[{ id: c:1, src: 'q', "the x": 'q' }]"""

[[test.results]]
value = """[{ id: c:2, src: 'other', "the x": 'other' }]"""

[[test.results]]
value = "['q']"
*/

DEFINE TABLE log SCHEMALESS;
DEFINE TABLE b SCHEMALESS;
DEFINE FIELD src ON b TYPE option<string>;
DEFINE FIELD plain   ON b COMPUTED string::concat(src, '/plain');
DEFINE FIELD `the x` ON b COMPUTED string::concat(src, '/space');
DEFINE FIELD `a-b`   ON b COMPUTED string::concat(src, '/hyphen');
DEFINE FIELD `café`  ON b COMPUTED string::concat(src, '/unicode');
DEFINE FIELD `1st`   ON b COMPUTED string::concat(src, '/digit');

DEFINE EVENT e_plain ON b WHEN plain != NONE
	THEN { CREATE log SET which = 'plain', ev = $event, now = $after.plain, was = $before.plain };
DEFINE EVENT e_space ON b WHEN `the x` != NONE
	THEN { CREATE log SET which = 'the x', ev = $event, now = $after.`the x`, was = $before.`the x` };
DEFINE EVENT e_hyphen ON b WHEN `a-b` != NONE
	THEN { CREATE log SET which = 'a-b', ev = $event, now = $after.`a-b`, was = $before.`a-b` };
DEFINE EVENT e_unicode ON b WHEN `café` != NONE
	THEN { CREATE log SET which = 'café', ev = $event, now = $after.`café`, was = $before.`café` };
DEFINE EVENT e_digit ON b WHEN `1st` != NONE
	THEN { CREATE log SET which = '1st', ev = $event, now = $after.`1st`, was = $before.`1st` };

CREATE b:1 SET src = 'q';
UPDATE b:1 SET src = 'r';
DELETE b:1;

SELECT which, ev, now, was FROM log ORDER BY which, ev;

-- A condition on one of the escaped names alone is judged on the derived value.
DEFINE TABLE hits SCHEMALESS;
DEFINE TABLE c SCHEMALESS;
DEFINE FIELD src ON c TYPE option<string>;
DEFINE FIELD `the x` ON c COMPUTED src;
DEFINE EVENT only_q ON c WHEN `the x` = 'q' THEN { CREATE hits SET src = $after.src };
CREATE c:1 SET src = 'q';
CREATE c:2 SET src = 'other';
SELECT VALUE src FROM hits;
