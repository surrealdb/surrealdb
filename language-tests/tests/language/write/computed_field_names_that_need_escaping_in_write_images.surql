/**
[env]
namespace = true
database = true

[test]
reason = """
A `COMPUTED` field is never stored, so a write has to derive it onto the images it
hands back (`RETURN BEFORE` / `AFTER` / `DIFF`) under the key the record would hold
it by. A field name that has to be backticked when written (a space, a hyphen, a
non-ASCII character, a leading digit, a dot) must be keyed by the name itself, not by
the backticked text it is rendered as. `plain` and `MixedCase` need no escaping and
are the controls: every field is derived the same way, so a name that is keyed
differently shows up as a different key, or as a missing one.

The same fields are written on two tables. A record that something observes (here an
event) has both its images derived up front, ahead of the observer, which is a
separate path from the one a record nobody observes is written by; `p` is not
observed, `o` is, and the two must answer alike.
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
value = """[{ "1st": 'q/digit', MixedCase: 'q/mixed', "a-b": 'q/hyphen', "a.b": 'q/dot', "café": 'q/unicode', id: p:1, plain: 'q/plain', select: 'q/keyword', src: 'q', "the x": 'q/space' }]"""

[[test.results]]
value = """[{ "1st": 'q/digit', MixedCase: 'q/mixed', "a-b": 'q/hyphen', "a.b": 'q/dot', "café": 'q/unicode', id: p:1, plain: 'q/plain', select: 'q/keyword', src: 'q', "the x": 'q/space' }]"""

[[test.results]]
value = """[{ "1st": 's/digit', MixedCase: 's/mixed', "a-b": 's/hyphen', "a.b": 's/dot', "café": 's/unicode', id: p:1, plain: 's/plain', select: 's/keyword', src: 's', "the x": 's/space' }]"""

[[test.results]]
value = """[{ "1st": 'u/digit', MixedCase: 'u/mixed', "a-b": 'u/hyphen', "a.b": 'u/dot', "café": 'u/unicode', id: p:2, plain: 'u/plain', select: 'u/keyword', src: 'u', "the x": 'u/space' }]"""

[[test.results]]
value = '''[[{ op: 'change', path: '/1st', value: '@@ -1,7 +1,8 @@\n u\n+2\n /digit\n' }, { op: 'change', path: '/MixedCase', value: '@@ -1,7 +1,8 @@\n u\n+2\n /mixed\n' }, { op: 'change', path: '/a-b', value: '@@ -1,8 +1,9 @@\n u\n+2\n /hyphen\n' }, { op: 'change', path: '/a.b', value: '@@ -1,5 +1,6 @@\n u\n+2\n /dot\n' }, { op: 'change', path: '/café', value: '@@ -1,9 +1,10 @@\n u\n+2\n /unicode\n' }, { op: 'change', path: '/plain', value: '@@ -1,7 +1,8 @@\n u\n+2\n /plain\n' }, { op: 'change', path: '/select', value: '@@ -1,9 +1,10 @@\n u\n+2\n /keyword\n' }, { op: 'change', path: '/src', value: '@@ -1,1 +1,2 @@\n u\n+2\n' }, { op: 'change', path: '/the x', value: '@@ -1,7 +1,8 @@\n u\n+2\n /space\n' }]]'''

[[test.results]]
value = """[{ "1st": 'v/digit', MixedCase: 'v/mixed', "a-b": 'v/hyphen', "a.b": 'v/dot', "café": 'v/unicode', id: p:3, plain: 'v/plain', select: 'v/keyword', src: 'v', "the x": 'v/space' }]"""

[[test.results]]
value = """[{ "1st": 'v/digit', MixedCase: 'v/mixed', "a-b": 'v/hyphen', "a.b": 'v/dot', "café": 'v/unicode', id: p:3, plain: 'v/plain', select: 'v/keyword', src: 'v', "the x": 'v/space' }]"""

[[test.results]]
value = """[{ "1st": 'q/digit', MixedCase: 'q/mixed', "a-b": 'q/hyphen', "a.b": 'q/dot', "café": 'q/unicode', id: o:1, plain: 'q/plain', select: 'q/keyword', src: 'q', "the x": 'q/space' }]"""

[[test.results]]
value = """[{ "1st": 'q/digit', MixedCase: 'q/mixed', "a-b": 'q/hyphen', "a.b": 'q/dot', "café": 'q/unicode', id: o:1, plain: 'q/plain', select: 'q/keyword', src: 'q', "the x": 'q/space' }]"""

[[test.results]]
value = """[{ "1st": 's/digit', MixedCase: 's/mixed', "a-b": 's/hyphen', "a.b": 's/dot', "café": 's/unicode', id: o:1, plain: 's/plain', select: 's/keyword', src: 's', "the x": 's/space' }]"""

[[test.results]]
value = """[{ "1st": 'u/digit', MixedCase: 'u/mixed', "a-b": 'u/hyphen', "a.b": 'u/dot', "café": 'u/unicode', id: o:2, plain: 'u/plain', select: 'u/keyword', src: 'u', "the x": 'u/space' }]"""

[[test.results]]
value = '''[[{ op: 'change', path: '/1st', value: '@@ -1,7 +1,8 @@\n u\n+2\n /digit\n' }, { op: 'change', path: '/MixedCase', value: '@@ -1,7 +1,8 @@\n u\n+2\n /mixed\n' }, { op: 'change', path: '/a-b', value: '@@ -1,8 +1,9 @@\n u\n+2\n /hyphen\n' }, { op: 'change', path: '/a.b', value: '@@ -1,5 +1,6 @@\n u\n+2\n /dot\n' }, { op: 'change', path: '/café', value: '@@ -1,9 +1,10 @@\n u\n+2\n /unicode\n' }, { op: 'change', path: '/plain', value: '@@ -1,7 +1,8 @@\n u\n+2\n /plain\n' }, { op: 'change', path: '/select', value: '@@ -1,9 +1,10 @@\n u\n+2\n /keyword\n' }, { op: 'change', path: '/src', value: '@@ -1,1 +1,2 @@\n u\n+2\n' }, { op: 'change', path: '/the x', value: '@@ -1,7 +1,8 @@\n u\n+2\n /space\n' }]]'''

[[test.results]]
value = """[{ "1st": 'v/digit', MixedCase: 'v/mixed', "a-b": 'v/hyphen', "a.b": 'v/dot', "café": 'v/unicode', id: o:3, plain: 'v/plain', select: 'v/keyword', src: 'v', "the x": 'v/space' }]"""

[[test.results]]
value = """[{ "1st": 'v/digit', MixedCase: 'v/mixed', "a-b": 'v/hyphen', "a.b": 'v/dot', "café": 'v/unicode', id: o:3, plain: 'v/plain', select: 'v/keyword', src: 'v', "the x": 'v/space' }]"""

[[test.results]]
value = """[{ "1st": 'w/digit', "a-b": 'w/hyphen', "a.b": 'w/dot', "café": 'w/unicode', "the x": 'w/space' }]"""

[[test.results]]
value = """[{ "1st": 'w/digit', "a-b": 'w/hyphen', "a.b": 'w/dot', "café": 'w/unicode', "the x": 'w/space' }]"""

[[test.results]]
value = "['x/space']"

[[test.results]]
value = "['x/space']"

[[test.results]]
value = "[{ hyphen: 'y/hyphen!', spaced: 'y/space', src: 'y' }]"

[[test.results]]
value = "[{ hyphen: 'y/hyphen!', spaced: 'y/space', src: 'y' }]"

[[test.results]]
value = """[{ "1st": 'x/digit', MixedCase: 'x/mixed', "a-b": 'x/hyphen', "a.b": 'x/dot', "café": 'x/unicode', id: p:1, plain: 'x/plain', select: 'x/keyword', src: 'x', "the x": 'x/space' }, { "1st": 'u2/digit', MixedCase: 'u2/mixed', "a-b": 'u2/hyphen', "a.b": 'u2/dot', "café": 'u2/unicode', id: p:2, plain: 'u2/plain', select: 'u2/keyword', src: 'u2', "the x": 'u2/space' }, { "1st": 'y/digit', MixedCase: 'y/mixed', "a-b": 'y/hyphen', "a.b": 'y/dot', "café": 'y/unicode', id: p:4, plain: 'y/plain', select: 'y/keyword', src: 'y', "the x": 'y/space' }]"""

[[test.results]]
value = """[{ "1st": 'x/digit', MixedCase: 'x/mixed', "a-b": 'x/hyphen', "a.b": 'x/dot', "café": 'x/unicode', id: o:1, plain: 'x/plain', select: 'x/keyword', src: 'x', "the x": 'x/space' }, { "1st": 'u2/digit', MixedCase: 'u2/mixed', "a-b": 'u2/hyphen', "a.b": 'u2/dot', "café": 'u2/unicode', id: o:2, plain: 'u2/plain', select: 'u2/keyword', src: 'u2', "the x": 'u2/space' }, { "1st": 'y/digit', MixedCase: 'y/mixed', "a-b": 'y/hyphen', "a.b": 'y/dot', "café": 'y/unicode', id: o:4, plain: 'y/plain', select: 'y/keyword', src: 'y', "the x": 'y/space' }]"""
*/

DEFINE TABLE p SCHEMALESS;
DEFINE FIELD src ON p TYPE option<string>;
DEFINE FIELD plain     ON p COMPUTED string::concat(src, '/plain');
DEFINE FIELD MixedCase ON p COMPUTED string::concat(src, '/mixed');
DEFINE FIELD `the x`   ON p COMPUTED string::concat(src, '/space');
DEFINE FIELD `a-b`     ON p COMPUTED string::concat(src, '/hyphen');
DEFINE FIELD `café`    ON p COMPUTED string::concat(src, '/unicode');
DEFINE FIELD `1st`     ON p COMPUTED string::concat(src, '/digit');
DEFINE FIELD `a.b`     ON p COMPUTED string::concat(src, '/dot');
DEFINE FIELD `select`  ON p COMPUTED string::concat(src, '/keyword');

DEFINE TABLE o SCHEMALESS;
DEFINE FIELD src ON o TYPE option<string>;
DEFINE FIELD plain     ON o COMPUTED string::concat(src, '/plain');
DEFINE FIELD MixedCase ON o COMPUTED string::concat(src, '/mixed');
DEFINE FIELD `the x`   ON o COMPUTED string::concat(src, '/space');
DEFINE FIELD `a-b`     ON o COMPUTED string::concat(src, '/hyphen');
DEFINE FIELD `café`    ON o COMPUTED string::concat(src, '/unicode');
DEFINE FIELD `1st`     ON o COMPUTED string::concat(src, '/digit');
DEFINE FIELD `a.b`     ON o COMPUTED string::concat(src, '/dot');
DEFINE FIELD `select`  ON o COMPUTED string::concat(src, '/keyword');
DEFINE EVENT observed ON o WHEN false THEN {};

-- Every write statement's image carries each derived field under its own name.
CREATE p:1 SET src = 'q' RETURN AFTER;
UPDATE p:1 SET src = 'r' RETURN BEFORE;
UPDATE p:1 SET src = 's' RETURN AFTER;
UPSERT p:2 SET src = 'u';
UPDATE p:2 SET src = 'u2' RETURN DIFF;
INSERT INTO p { id: 3, src: 'v' };
DELETE p:3 RETURN BEFORE;

CREATE o:1 SET src = 'q' RETURN AFTER;
UPDATE o:1 SET src = 'r' RETURN BEFORE;
UPDATE o:1 SET src = 's' RETURN AFTER;
UPSERT o:2 SET src = 'u';
UPDATE o:2 SET src = 'u2' RETURN DIFF;
INSERT INTO o { id: 3, src: 'v' };
DELETE o:3 RETURN BEFORE;

-- A projected image names them the same way, so the columns can be asked for.
UPDATE p:1 SET src = 'w' RETURN `the x`, `a-b`, `café`, `1st`, `a.b`;
UPDATE o:1 SET src = 'w' RETURN `the x`, `a-b`, `café`, `1st`, `a.b`;
UPDATE p:1 SET src = 'x' RETURN VALUE `the x`;
UPDATE o:1 SET src = 'x' RETURN VALUE `the x`;
CREATE p:4 SET src = 'y' RETURN src, `the x` AS spaced, `a-b` + '!' AS hyphen;
CREATE o:4 SET src = 'y' RETURN src, `the x` AS spaced, `a-b` + '!' AS hyphen;

-- A read agrees with what the writes returned.
SELECT * FROM p ORDER BY id;
SELECT * FROM o ORDER BY id;
