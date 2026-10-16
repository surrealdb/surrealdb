/**
[env]
imports = ["./return_images_move_import.surql"]
timeout = 30000
auth = { level = "owner" }

[test]
reason = """
The output stage shows the images of the record the write left, for a table with
no field view as the images themselves and for a table whose view derives or cuts a
field through that view. Every form of write, under the default output and each
named one, answers the images the record held, as root: the record it created, its
pre-image, the changes between the two, and what a projection reads from both. An
UPSERT, or an INSERT with ON DUPLICATE KEY UPDATE, that finds the record stored runs
as an update of it, evaluates its clause against the stored record, and shows the
images of the update.
"""

[[test.results]]
value = "[{ id: plain:1, n: 1, secret: 's1' }]"

[[test.results]]
value = "[{ id: plain:2, n: 2, secret: 's2' }]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[[{ op: 'replace', path: '', value: { id: plain:4, n: 4, secret: 's4' } }]]"

[[test.results]]
value = "[{ id: plain:5, n: 5, secret: 's5' }]"

[[test.results]]
value = '[{ "$after.n": 6, "$before.n": NONE, id: plain:6 }]'

[[test.results]]
value = "[{ id: plain:1, n: 11, secret: 's1' }]"

[[test.results]]
value = "[{ id: plain:2, n: 12, secret: 's2' }]"

[[test.results]]
value = "[{ id: plain:3, n: 3, secret: 's3' }]"

[[test.results]]
value = "[[{ op: 'replace', path: '/n', value: 14 }]]"

[[test.results]]
value = "[{ id: plain:5, n: 15, secret: 's5' }]"

[[test.results]]
value = '[{ "$after.n": 16, "$before.n": 6, id: plain:6 }]'

[[test.results]]
value = "[{ id: plain:1, n: 21, secret: 's1' }]"

[[test.results]]
value = "[{ id: plain:2, n: 22, secret: 's2' }]"

[[test.results]]
value = "[{ id: plain:3, n: 13, secret: 's3' }]"

[[test.results]]
value = "[[{ op: 'replace', path: '/n', value: 24 }]]"

[[test.results]]
value = "[{ id: plain:5, n: 25, secret: 's5' }]"

[[test.results]]
value = '[{ "$after.n": 26, "$before.n": 16, id: plain:6 }]'

[[test.results]]
value = "[{ id: plain:11, n: 11, secret: 's11' }]"

[[test.results]]
value = "[{ id: plain:12, n: 12, secret: 's12' }]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[[{ op: 'replace', path: '', value: { id: plain:14, n: 14, secret: 's14' } }]]"

[[test.results]]
value = "[{ id: plain:15, n: 15, secret: 's15' }]"

[[test.results]]
value = '[{ "$after.n": 16, "$before.n": NONE, id: plain:16 }]'

[[test.results]]
value = "[{ id: plain:21, n: 21, secret: 's21' }]"

[[test.results]]
value = "[{ id: plain:22, n: 22, secret: 's22' }]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[[{ op: 'replace', path: '', value: { id: plain:24, n: 24, secret: 's24' } }]]"

[[test.results]]
value = "[{ id: plain:25, n: 25, secret: 's25' }]"

[[test.results]]
value = '[{ "$after.n": 26, "$before.n": NONE, id: plain:26 }]'

[[test.results]]
value = "[{ id: plain:21, n: 121, secret: 's21' }]"

[[test.results]]
value = "[{ id: plain:22, n: 122, secret: 's22' }]"

[[test.results]]
value = "[{ id: plain:23, n: 23, secret: 's23' }]"

[[test.results]]
value = "[[{ op: 'replace', path: '/n', value: 124 }]]"

[[test.results]]
value = "[{ id: plain:25, n: 125, secret: 's25' }]"

[[test.results]]
value = '[{ "$after.n": 126, "$before.n": 26, id: plain:26 }]'

[[test.results]]
value = "[{ id: e_plain:1, in: plain:1, n: 1, out: plain:2 }]"

[[test.results]]
value = "[{ id: e_plain:2, in: plain:1, n: 2, out: plain:2 }]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[[{ op: 'replace', path: '', value: { id: e_plain:4, in: plain:1, n: 4, out: plain:2 } }]]"

[[test.results]]
value = "[{ id: e_plain:5, in: plain:1, n: 5, out: plain:2 }]"

[[test.results]]
value = '[{ "$after.n": 6, "$before.n": NONE, id: e_plain:6 }]'

[[test.results]]
value = "[{ id: e_plain:1, in: plain:1, n: 51, out: plain:2 }]"

[[test.results]]
value = "[{ id: e_plain:2, in: plain:1, n: 52, out: plain:2 }]"

[[test.results]]
value = "[{ id: e_plain:3, in: plain:1, n: 3, out: plain:2 }]"

[[test.results]]
value = "[[{ op: 'replace', path: '/n', value: 54 }]]"

[[test.results]]
value = "[{ id: e_plain:5, in: plain:1, n: 55, out: plain:2 }]"

[[test.results]]
value = '[{ "$after.n": 56, "$before.n": 6, id: e_plain:6 }]'

[[test.results]]
value = "[]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[{ id: plain:3, n: 23, secret: 's3' }]"

[[test.results]]
value = "[[{ op: 'replace', path: '', value: NONE }]]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = '[{ "$after.n": NONE, "$before.n": 26, id: NONE }]'

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: plain:11, n: 11, secret: 's11' }, { id: plain:12, n: 12, secret: 's12' }, { id: plain:13, n: 13, secret: 's13' }, { id: plain:14, n: 14, secret: 's14' }, { id: plain:15, n: 15, secret: 's15' }, { id: plain:16, n: 16, secret: 's16' }, { id: plain:21, n: 121, secret: 's21' }, { id: plain:22, n: 122, secret: 's22' }, { id: plain:23, n: 123, secret: 's23' }, { id: plain:24, n: 124, secret: 's24' }, { id: plain:25, n: 125, secret: 's25' }, { id: plain:26, n: 126, secret: 's26' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ double: 2, id: comp:1, n: 1, secret: 's1' }]"

[[test.results]]
value = "[{ double: 4, id: comp:2, n: 2, secret: 's2' }]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[[{ op: 'replace', path: '', value: { double: 8, id: comp:4, n: 4, secret: 's4' } }]]"

[[test.results]]
value = "[{ double: 10, id: comp:5, n: 5, secret: 's5' }]"

[[test.results]]
value = '[{ "$after.n": 6, "$before.n": NONE, id: comp:6 }]'

[[test.results]]
value = "[{ double: 22, id: comp:1, n: 11, secret: 's1' }]"

[[test.results]]
value = "[{ double: 24, id: comp:2, n: 12, secret: 's2' }]"

[[test.results]]
value = "[{ double: 6, id: comp:3, n: 3, secret: 's3' }]"

[[test.results]]
value = "[[{ op: 'replace', path: '/double', value: 28 }, { op: 'replace', path: '/n', value: 14 }]]"

[[test.results]]
value = "[{ double: 30, id: comp:5, n: 15, secret: 's5' }]"

[[test.results]]
value = '[{ "$after.n": 16, "$before.n": 6, id: comp:6 }]'

[[test.results]]
value = "[{ double: 42, id: comp:1, n: 21, secret: 's1' }]"

[[test.results]]
value = "[{ double: 44, id: comp:2, n: 22, secret: 's2' }]"

[[test.results]]
value = "[{ double: 26, id: comp:3, n: 13, secret: 's3' }]"

[[test.results]]
value = "[[{ op: 'replace', path: '/double', value: 48 }, { op: 'replace', path: '/n', value: 24 }]]"

[[test.results]]
value = "[{ double: 50, id: comp:5, n: 25, secret: 's5' }]"

[[test.results]]
value = '[{ "$after.n": 26, "$before.n": 16, id: comp:6 }]'

[[test.results]]
value = "[{ double: 22, id: comp:11, n: 11, secret: 's11' }]"

[[test.results]]
value = "[{ double: 24, id: comp:12, n: 12, secret: 's12' }]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[[{ op: 'replace', path: '', value: { double: 28, id: comp:14, n: 14, secret: 's14' } }]]"

[[test.results]]
value = "[{ double: 30, id: comp:15, n: 15, secret: 's15' }]"

[[test.results]]
value = '[{ "$after.n": 16, "$before.n": NONE, id: comp:16 }]'

[[test.results]]
value = "[{ double: 42, id: comp:21, n: 21, secret: 's21' }]"

[[test.results]]
value = "[{ double: 44, id: comp:22, n: 22, secret: 's22' }]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[[{ op: 'replace', path: '', value: { double: 48, id: comp:24, n: 24, secret: 's24' } }]]"

[[test.results]]
value = "[{ double: 50, id: comp:25, n: 25, secret: 's25' }]"

[[test.results]]
value = '[{ "$after.n": 26, "$before.n": NONE, id: comp:26 }]'

[[test.results]]
value = "[{ double: 242, id: comp:21, n: 121, secret: 's21' }]"

[[test.results]]
value = "[{ double: 244, id: comp:22, n: 122, secret: 's22' }]"

[[test.results]]
value = "[{ double: 46, id: comp:23, n: 23, secret: 's23' }]"

[[test.results]]
value = "[[{ op: 'replace', path: '/double', value: 248 }, { op: 'replace', path: '/n', value: 124 }]]"

[[test.results]]
value = "[{ double: 250, id: comp:25, n: 125, secret: 's25' }]"

[[test.results]]
value = '[{ "$after.n": 126, "$before.n": 26, id: comp:26 }]'

[[test.results]]
value = "[{ id: e_comp:1, in: comp:1, n: 1, out: comp:2 }]"

[[test.results]]
value = "[{ id: e_comp:2, in: comp:1, n: 2, out: comp:2 }]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[[{ op: 'replace', path: '', value: { id: e_comp:4, in: comp:1, n: 4, out: comp:2 } }]]"

[[test.results]]
value = "[{ id: e_comp:5, in: comp:1, n: 5, out: comp:2 }]"

[[test.results]]
value = '[{ "$after.n": 6, "$before.n": NONE, id: e_comp:6 }]'

[[test.results]]
value = "[{ id: e_comp:1, in: comp:1, n: 51, out: comp:2 }]"

[[test.results]]
value = "[{ id: e_comp:2, in: comp:1, n: 52, out: comp:2 }]"

[[test.results]]
value = "[{ id: e_comp:3, in: comp:1, n: 3, out: comp:2 }]"

[[test.results]]
value = "[[{ op: 'replace', path: '/n', value: 54 }]]"

[[test.results]]
value = "[{ id: e_comp:5, in: comp:1, n: 55, out: comp:2 }]"

[[test.results]]
value = '[{ "$after.n": 56, "$before.n": 6, id: e_comp:6 }]'

[[test.results]]
value = "[]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[{ double: 46, id: comp:3, n: 23, secret: 's3' }]"

[[test.results]]
value = "[[{ op: 'replace', path: '', value: NONE }]]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = '[{ "$after.n": NONE, "$before.n": 26, id: NONE }]'

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ double: 22, id: comp:11, n: 11, secret: 's11' }, { double: 24, id: comp:12, n: 12, secret: 's12' }, { double: 26, id: comp:13, n: 13, secret: 's13' }, { double: 28, id: comp:14, n: 14, secret: 's14' }, { double: 30, id: comp:15, n: 15, secret: 's15' }, { double: 32, id: comp:16, n: 16, secret: 's16' }, { double: 242, id: comp:21, n: 121, secret: 's21' }, { double: 244, id: comp:22, n: 122, secret: 's22' }, { double: 246, id: comp:23, n: 123, secret: 's23' }, { double: 248, id: comp:24, n: 124, secret: 's24' }, { double: 250, id: comp:25, n: 125, secret: 's25' }, { double: 252, id: comp:26, n: 126, secret: 's26' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: cut:1, n: 1, secret: 's1' }]"

[[test.results]]
value = "[{ id: cut:2, n: 2, secret: 's2' }]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[[{ op: 'replace', path: '', value: { id: cut:4, n: 4, secret: 's4' } }]]"

[[test.results]]
value = "[{ id: cut:5, n: 5, secret: 's5' }]"

[[test.results]]
value = '[{ "$after.n": 6, "$before.n": NONE, id: cut:6 }]'

[[test.results]]
value = "[{ id: cut:1, n: 11, secret: 's1' }]"

[[test.results]]
value = "[{ id: cut:2, n: 12, secret: 's2' }]"

[[test.results]]
value = "[{ id: cut:3, n: 3, secret: 's3' }]"

[[test.results]]
value = "[[{ op: 'replace', path: '/n', value: 14 }]]"

[[test.results]]
value = "[{ id: cut:5, n: 15, secret: 's5' }]"

[[test.results]]
value = '[{ "$after.n": 16, "$before.n": 6, id: cut:6 }]'

[[test.results]]
value = "[{ id: cut:1, n: 21, secret: 's1' }]"

[[test.results]]
value = "[{ id: cut:2, n: 22, secret: 's2' }]"

[[test.results]]
value = "[{ id: cut:3, n: 13, secret: 's3' }]"

[[test.results]]
value = "[[{ op: 'replace', path: '/n', value: 24 }]]"

[[test.results]]
value = "[{ id: cut:5, n: 25, secret: 's5' }]"

[[test.results]]
value = '[{ "$after.n": 26, "$before.n": 16, id: cut:6 }]'

[[test.results]]
value = "[{ id: cut:11, n: 11, secret: 's11' }]"

[[test.results]]
value = "[{ id: cut:12, n: 12, secret: 's12' }]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[[{ op: 'replace', path: '', value: { id: cut:14, n: 14, secret: 's14' } }]]"

[[test.results]]
value = "[{ id: cut:15, n: 15, secret: 's15' }]"

[[test.results]]
value = '[{ "$after.n": 16, "$before.n": NONE, id: cut:16 }]'

[[test.results]]
value = "[{ id: cut:21, n: 21, secret: 's21' }]"

[[test.results]]
value = "[{ id: cut:22, n: 22, secret: 's22' }]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[[{ op: 'replace', path: '', value: { id: cut:24, n: 24, secret: 's24' } }]]"

[[test.results]]
value = "[{ id: cut:25, n: 25, secret: 's25' }]"

[[test.results]]
value = '[{ "$after.n": 26, "$before.n": NONE, id: cut:26 }]'

[[test.results]]
value = "[{ id: cut:21, n: 121, secret: 's21' }]"

[[test.results]]
value = "[{ id: cut:22, n: 122, secret: 's22' }]"

[[test.results]]
value = "[{ id: cut:23, n: 23, secret: 's23' }]"

[[test.results]]
value = "[[{ op: 'replace', path: '/n', value: 124 }]]"

[[test.results]]
value = "[{ id: cut:25, n: 125, secret: 's25' }]"

[[test.results]]
value = '[{ "$after.n": 126, "$before.n": 26, id: cut:26 }]'

[[test.results]]
value = "[{ id: e_cut:1, in: cut:1, n: 1, out: cut:2 }]"

[[test.results]]
value = "[{ id: e_cut:2, in: cut:1, n: 2, out: cut:2 }]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[[{ op: 'replace', path: '', value: { id: e_cut:4, in: cut:1, n: 4, out: cut:2 } }]]"

[[test.results]]
value = "[{ id: e_cut:5, in: cut:1, n: 5, out: cut:2 }]"

[[test.results]]
value = '[{ "$after.n": 6, "$before.n": NONE, id: e_cut:6 }]'

[[test.results]]
value = "[{ id: e_cut:1, in: cut:1, n: 51, out: cut:2 }]"

[[test.results]]
value = "[{ id: e_cut:2, in: cut:1, n: 52, out: cut:2 }]"

[[test.results]]
value = "[{ id: e_cut:3, in: cut:1, n: 3, out: cut:2 }]"

[[test.results]]
value = "[[{ op: 'replace', path: '/n', value: 54 }]]"

[[test.results]]
value = "[{ id: e_cut:5, in: cut:1, n: 55, out: cut:2 }]"

[[test.results]]
value = '[{ "$after.n": 56, "$before.n": 6, id: e_cut:6 }]'

[[test.results]]
value = "[]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[{ id: cut:3, n: 23, secret: 's3' }]"

[[test.results]]
value = "[[{ op: 'replace', path: '', value: NONE }]]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = '[{ "$after.n": NONE, "$before.n": 26, id: NONE }]'

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: cut:11, n: 11, secret: 's11' }, { id: cut:12, n: 12, secret: 's12' }, { id: cut:13, n: 13, secret: 's13' }, { id: cut:14, n: 14, secret: 's14' }, { id: cut:15, n: 15, secret: 's15' }, { id: cut:16, n: 16, secret: 's16' }, { id: cut:21, n: 121, secret: 's21' }, { id: cut:22, n: 122, secret: 's22' }, { id: cut:23, n: 123, secret: 's23' }, { id: cut:24, n: 124, secret: 's24' }, { id: cut:25, n: 125, secret: 's25' }, { id: cut:26, n: 126, secret: 's26' }]"

[[test.results]]
value = "[]"
*/

-- plain
CREATE plain:1 SET n = 1, secret = 's1';
CREATE plain:2 SET n = 2, secret = 's2' RETURN AFTER;
CREATE plain:3 SET n = 3, secret = 's3' RETURN BEFORE;
CREATE plain:4 SET n = 4, secret = 's4' RETURN DIFF;
CREATE plain:5 SET n = 5, secret = 's5' RETURN *;
CREATE plain:6 SET n = 6, secret = 's6' RETURN id, $before.n, $after.n;
UPDATE plain:1 SET n = n + 10;
UPDATE plain:2 SET n = n + 10 RETURN AFTER;
UPDATE plain:3 SET n = n + 10 RETURN BEFORE;
UPDATE plain:4 SET n = n + 10 RETURN DIFF;
UPDATE plain:5 SET n = n + 10 RETURN *;
UPDATE plain:6 SET n = n + 10 RETURN id, $before.n, $after.n;
UPSERT plain:1 SET n = n + 10;
UPSERT plain:2 SET n = n + 10 RETURN AFTER;
UPSERT plain:3 SET n = n + 10 RETURN BEFORE;
UPSERT plain:4 SET n = n + 10 RETURN DIFF;
UPSERT plain:5 SET n = n + 10 RETURN *;
UPSERT plain:6 SET n = n + 10 RETURN id, $before.n, $after.n;
UPSERT plain:11 SET n = 11, secret = 's11';
UPSERT plain:12 SET n = 12, secret = 's12' RETURN AFTER;
UPSERT plain:13 SET n = 13, secret = 's13' RETURN BEFORE;
UPSERT plain:14 SET n = 14, secret = 's14' RETURN DIFF;
UPSERT plain:15 SET n = 15, secret = 's15' RETURN *;
UPSERT plain:16 SET n = 16, secret = 's16' RETURN id, $before.n, $after.n;
INSERT INTO plain { id: plain:21, n: 21, secret: 's21' };
INSERT INTO plain { id: plain:22, n: 22, secret: 's22' } RETURN AFTER;
INSERT INTO plain { id: plain:23, n: 23, secret: 's23' } RETURN BEFORE;
INSERT INTO plain { id: plain:24, n: 24, secret: 's24' } RETURN DIFF;
INSERT INTO plain { id: plain:25, n: 25, secret: 's25' } RETURN *;
INSERT INTO plain { id: plain:26, n: 26, secret: 's26' } RETURN id, $before.n, $after.n;
INSERT INTO plain { id: plain:21, n: 0 } ON DUPLICATE KEY UPDATE n = n + 100;
INSERT INTO plain { id: plain:22, n: 0 } ON DUPLICATE KEY UPDATE n = n + 100 RETURN AFTER;
INSERT INTO plain { id: plain:23, n: 0 } ON DUPLICATE KEY UPDATE n = n + 100 RETURN BEFORE;
INSERT INTO plain { id: plain:24, n: 0 } ON DUPLICATE KEY UPDATE n = n + 100 RETURN DIFF;
INSERT INTO plain { id: plain:25, n: 0 } ON DUPLICATE KEY UPDATE n = n + 100 RETURN *;
INSERT INTO plain { id: plain:26, n: 0 } ON DUPLICATE KEY UPDATE n = n + 100 RETURN id, $before.n, $after.n;
RELATE plain:1->e_plain:1->plain:2 SET n = 1;
RELATE plain:1->e_plain:2->plain:2 SET n = 2 RETURN AFTER;
RELATE plain:1->e_plain:3->plain:2 SET n = 3 RETURN BEFORE;
RELATE plain:1->e_plain:4->plain:2 SET n = 4 RETURN DIFF;
RELATE plain:1->e_plain:5->plain:2 SET n = 5 RETURN *;
RELATE plain:1->e_plain:6->plain:2 SET n = 6 RETURN id, $before.n, $after.n;
RELATE plain:1->e_plain:1->plain:2 SET n = n + 50;
RELATE plain:1->e_plain:2->plain:2 SET n = n + 50 RETURN AFTER;
RELATE plain:1->e_plain:3->plain:2 SET n = n + 50 RETURN BEFORE;
RELATE plain:1->e_plain:4->plain:2 SET n = n + 50 RETURN DIFF;
RELATE plain:1->e_plain:5->plain:2 SET n = n + 50 RETURN *;
RELATE plain:1->e_plain:6->plain:2 SET n = n + 50 RETURN id, $before.n, $after.n;
DELETE plain:1;
DELETE plain:2 RETURN AFTER;
DELETE plain:3 RETURN BEFORE;
DELETE plain:4 RETURN DIFF;
DELETE plain:5 RETURN *;
DELETE plain:6 RETURN id, $before.n, $after.n;
DELETE e_plain:1;
DELETE e_plain:2 RETURN AFTER;
DELETE e_plain:3 RETURN BEFORE;
DELETE e_plain:4 RETURN DIFF;
DELETE e_plain:5 RETURN *;
DELETE e_plain:6 RETURN id, $before.n, $after.n;
SELECT * FROM plain ORDER BY id;
SELECT * FROM e_plain ORDER BY id;
-- comp
CREATE comp:1 SET n = 1, secret = 's1';
CREATE comp:2 SET n = 2, secret = 's2' RETURN AFTER;
CREATE comp:3 SET n = 3, secret = 's3' RETURN BEFORE;
CREATE comp:4 SET n = 4, secret = 's4' RETURN DIFF;
CREATE comp:5 SET n = 5, secret = 's5' RETURN *;
CREATE comp:6 SET n = 6, secret = 's6' RETURN id, $before.n, $after.n;
UPDATE comp:1 SET n = n + 10;
UPDATE comp:2 SET n = n + 10 RETURN AFTER;
UPDATE comp:3 SET n = n + 10 RETURN BEFORE;
UPDATE comp:4 SET n = n + 10 RETURN DIFF;
UPDATE comp:5 SET n = n + 10 RETURN *;
UPDATE comp:6 SET n = n + 10 RETURN id, $before.n, $after.n;
UPSERT comp:1 SET n = n + 10;
UPSERT comp:2 SET n = n + 10 RETURN AFTER;
UPSERT comp:3 SET n = n + 10 RETURN BEFORE;
UPSERT comp:4 SET n = n + 10 RETURN DIFF;
UPSERT comp:5 SET n = n + 10 RETURN *;
UPSERT comp:6 SET n = n + 10 RETURN id, $before.n, $after.n;
UPSERT comp:11 SET n = 11, secret = 's11';
UPSERT comp:12 SET n = 12, secret = 's12' RETURN AFTER;
UPSERT comp:13 SET n = 13, secret = 's13' RETURN BEFORE;
UPSERT comp:14 SET n = 14, secret = 's14' RETURN DIFF;
UPSERT comp:15 SET n = 15, secret = 's15' RETURN *;
UPSERT comp:16 SET n = 16, secret = 's16' RETURN id, $before.n, $after.n;
INSERT INTO comp { id: comp:21, n: 21, secret: 's21' };
INSERT INTO comp { id: comp:22, n: 22, secret: 's22' } RETURN AFTER;
INSERT INTO comp { id: comp:23, n: 23, secret: 's23' } RETURN BEFORE;
INSERT INTO comp { id: comp:24, n: 24, secret: 's24' } RETURN DIFF;
INSERT INTO comp { id: comp:25, n: 25, secret: 's25' } RETURN *;
INSERT INTO comp { id: comp:26, n: 26, secret: 's26' } RETURN id, $before.n, $after.n;
INSERT INTO comp { id: comp:21, n: 0 } ON DUPLICATE KEY UPDATE n = n + 100;
INSERT INTO comp { id: comp:22, n: 0 } ON DUPLICATE KEY UPDATE n = n + 100 RETURN AFTER;
INSERT INTO comp { id: comp:23, n: 0 } ON DUPLICATE KEY UPDATE n = n + 100 RETURN BEFORE;
INSERT INTO comp { id: comp:24, n: 0 } ON DUPLICATE KEY UPDATE n = n + 100 RETURN DIFF;
INSERT INTO comp { id: comp:25, n: 0 } ON DUPLICATE KEY UPDATE n = n + 100 RETURN *;
INSERT INTO comp { id: comp:26, n: 0 } ON DUPLICATE KEY UPDATE n = n + 100 RETURN id, $before.n, $after.n;
RELATE comp:1->e_comp:1->comp:2 SET n = 1;
RELATE comp:1->e_comp:2->comp:2 SET n = 2 RETURN AFTER;
RELATE comp:1->e_comp:3->comp:2 SET n = 3 RETURN BEFORE;
RELATE comp:1->e_comp:4->comp:2 SET n = 4 RETURN DIFF;
RELATE comp:1->e_comp:5->comp:2 SET n = 5 RETURN *;
RELATE comp:1->e_comp:6->comp:2 SET n = 6 RETURN id, $before.n, $after.n;
RELATE comp:1->e_comp:1->comp:2 SET n = n + 50;
RELATE comp:1->e_comp:2->comp:2 SET n = n + 50 RETURN AFTER;
RELATE comp:1->e_comp:3->comp:2 SET n = n + 50 RETURN BEFORE;
RELATE comp:1->e_comp:4->comp:2 SET n = n + 50 RETURN DIFF;
RELATE comp:1->e_comp:5->comp:2 SET n = n + 50 RETURN *;
RELATE comp:1->e_comp:6->comp:2 SET n = n + 50 RETURN id, $before.n, $after.n;
DELETE comp:1;
DELETE comp:2 RETURN AFTER;
DELETE comp:3 RETURN BEFORE;
DELETE comp:4 RETURN DIFF;
DELETE comp:5 RETURN *;
DELETE comp:6 RETURN id, $before.n, $after.n;
DELETE e_comp:1;
DELETE e_comp:2 RETURN AFTER;
DELETE e_comp:3 RETURN BEFORE;
DELETE e_comp:4 RETURN DIFF;
DELETE e_comp:5 RETURN *;
DELETE e_comp:6 RETURN id, $before.n, $after.n;
SELECT * FROM comp ORDER BY id;
SELECT * FROM e_comp ORDER BY id;
-- cut
CREATE cut:1 SET n = 1, secret = 's1';
CREATE cut:2 SET n = 2, secret = 's2' RETURN AFTER;
CREATE cut:3 SET n = 3, secret = 's3' RETURN BEFORE;
CREATE cut:4 SET n = 4, secret = 's4' RETURN DIFF;
CREATE cut:5 SET n = 5, secret = 's5' RETURN *;
CREATE cut:6 SET n = 6, secret = 's6' RETURN id, $before.n, $after.n;
UPDATE cut:1 SET n = n + 10;
UPDATE cut:2 SET n = n + 10 RETURN AFTER;
UPDATE cut:3 SET n = n + 10 RETURN BEFORE;
UPDATE cut:4 SET n = n + 10 RETURN DIFF;
UPDATE cut:5 SET n = n + 10 RETURN *;
UPDATE cut:6 SET n = n + 10 RETURN id, $before.n, $after.n;
UPSERT cut:1 SET n = n + 10;
UPSERT cut:2 SET n = n + 10 RETURN AFTER;
UPSERT cut:3 SET n = n + 10 RETURN BEFORE;
UPSERT cut:4 SET n = n + 10 RETURN DIFF;
UPSERT cut:5 SET n = n + 10 RETURN *;
UPSERT cut:6 SET n = n + 10 RETURN id, $before.n, $after.n;
UPSERT cut:11 SET n = 11, secret = 's11';
UPSERT cut:12 SET n = 12, secret = 's12' RETURN AFTER;
UPSERT cut:13 SET n = 13, secret = 's13' RETURN BEFORE;
UPSERT cut:14 SET n = 14, secret = 's14' RETURN DIFF;
UPSERT cut:15 SET n = 15, secret = 's15' RETURN *;
UPSERT cut:16 SET n = 16, secret = 's16' RETURN id, $before.n, $after.n;
INSERT INTO cut { id: cut:21, n: 21, secret: 's21' };
INSERT INTO cut { id: cut:22, n: 22, secret: 's22' } RETURN AFTER;
INSERT INTO cut { id: cut:23, n: 23, secret: 's23' } RETURN BEFORE;
INSERT INTO cut { id: cut:24, n: 24, secret: 's24' } RETURN DIFF;
INSERT INTO cut { id: cut:25, n: 25, secret: 's25' } RETURN *;
INSERT INTO cut { id: cut:26, n: 26, secret: 's26' } RETURN id, $before.n, $after.n;
INSERT INTO cut { id: cut:21, n: 0 } ON DUPLICATE KEY UPDATE n = n + 100;
INSERT INTO cut { id: cut:22, n: 0 } ON DUPLICATE KEY UPDATE n = n + 100 RETURN AFTER;
INSERT INTO cut { id: cut:23, n: 0 } ON DUPLICATE KEY UPDATE n = n + 100 RETURN BEFORE;
INSERT INTO cut { id: cut:24, n: 0 } ON DUPLICATE KEY UPDATE n = n + 100 RETURN DIFF;
INSERT INTO cut { id: cut:25, n: 0 } ON DUPLICATE KEY UPDATE n = n + 100 RETURN *;
INSERT INTO cut { id: cut:26, n: 0 } ON DUPLICATE KEY UPDATE n = n + 100 RETURN id, $before.n, $after.n;
RELATE cut:1->e_cut:1->cut:2 SET n = 1;
RELATE cut:1->e_cut:2->cut:2 SET n = 2 RETURN AFTER;
RELATE cut:1->e_cut:3->cut:2 SET n = 3 RETURN BEFORE;
RELATE cut:1->e_cut:4->cut:2 SET n = 4 RETURN DIFF;
RELATE cut:1->e_cut:5->cut:2 SET n = 5 RETURN *;
RELATE cut:1->e_cut:6->cut:2 SET n = 6 RETURN id, $before.n, $after.n;
RELATE cut:1->e_cut:1->cut:2 SET n = n + 50;
RELATE cut:1->e_cut:2->cut:2 SET n = n + 50 RETURN AFTER;
RELATE cut:1->e_cut:3->cut:2 SET n = n + 50 RETURN BEFORE;
RELATE cut:1->e_cut:4->cut:2 SET n = n + 50 RETURN DIFF;
RELATE cut:1->e_cut:5->cut:2 SET n = n + 50 RETURN *;
RELATE cut:1->e_cut:6->cut:2 SET n = n + 50 RETURN id, $before.n, $after.n;
DELETE cut:1;
DELETE cut:2 RETURN AFTER;
DELETE cut:3 RETURN BEFORE;
DELETE cut:4 RETURN DIFF;
DELETE cut:5 RETURN *;
DELETE cut:6 RETURN id, $before.n, $after.n;
DELETE e_cut:1;
DELETE e_cut:2 RETURN AFTER;
DELETE e_cut:3 RETURN BEFORE;
DELETE e_cut:4 RETURN DIFF;
DELETE e_cut:5 RETURN *;
DELETE e_cut:6 RETURN id, $before.n, $after.n;
SELECT * FROM cut ORDER BY id;
SELECT * FROM e_cut ORDER BY id;
