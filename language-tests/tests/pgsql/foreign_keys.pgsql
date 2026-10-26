/**
[env]
namespace = true
database = true

[test]
reason = "A SQL foreign key makes its column a link to the referenced table that is written and read as a key: CREATE TABLE with a column and a table constraint, ALTER TABLE ADD FOREIGN KEY over a scalar column that holds rows (in the request that creates the table), a write that names no row of the target, a check of the rows that fails the ALTER, and the ON DELETE actions. SQL NULL written to a nullable key column is stored as NONE and reads back as NULL."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: 1, name: 'Eve' }, { id: 2, name: 'Sam' }]"

[[test.results]]
value = "[{ author: member:1, editor: member:2, id: 1 }, { author: member:2, editor: member:2, id: 2 }]"

[[test.results]]
error = 'An error occurred: insert or update on table "post" violates foreign key constraint "post_author_fkey"'

[[test.results]]
error = 'An error occurred: insert or update on table "post" violates foreign key constraint "post_editor_fkey"'

[[test.results]]
value = "[{ author: member:1, editor: member:2, id: 1, name: 'Eve' }, { author: member:2, editor: member:2, id: 2, name: 'Sam' }]"

[[test.results]]
value = "[{ id: 1 }, { id: 2 }]"

[[test.results]]
value = "[{ author: member:1, editor: member:1, id: 1 }]"

[[test.results]]
error = 'An error occurred: insert or update on table "post" violates foreign key constraint "post_author_fkey"'

[[test.results]]
value = "[{ author: 1i32, editor: 2i32, id: 1 }, { author: 2i32, editor: 1i32, id: 2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = 'An error occurred: insert or update on table "note" violates foreign key constraint "note_editor_fkey"'

[[test.results]]
value = "[{ author: member:1, editor: member:2, id: 1 }, { author: member:2, editor: member:1, id: 2 }]"

[[test.results]]
error = "Cannot delete `member:2` as it is referenced by `note:1` with an ON DELETE REJECT clause"

[[test.results]]
error = "Cannot delete `member:1` as it is referenced by `note:2` with an ON DELETE REJECT clause"

[[test.results]]
value = "[{ author: member:1, editor: member:1, id: 1 }, { author: member:2, editor: member:2, id: 2 }]"

[[test.results]]
value = "[{ author: member:1, editor: member:2, id: 1 }, { author: member:2, editor: member:1, id: 2 }]"

[[test.results]]
value = "[{ id: 1, name: 'Eve' }]"

[[test.results]]
value = "[{ author: member:2, editor: member:2, id: 2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: 1, ref: 5i32 }]"

[[test.results]]
error = 'An error occurred: insert or update on table "solo" violates foreign key constraint "solo_ref_fkey"'

[[test.results]]
value = "[{ id: 1, ref: 5i32 }]"

[[test.results]]
value = "[{ author: member:2, id: 5 }]"

[[test.results]]
value = "[{ author: member:2, id: 2 }]"

[[test.results]]
error = "Couldn't coerce value for field `author` of `post:6`: Expected `record<member>` but found `NULL`"

[[test.results]]
value = "[{ author: member:2, editor: NONE, id: 2 }, { author: member:2, editor: NONE, id: 5 }]"
*/

CREATE TABLE member (id INT PRIMARY KEY, name TEXT);
CREATE TABLE post (id INT PRIMARY KEY, author INT NOT NULL REFERENCES member(id) ON DELETE CASCADE, editor INT, FOREIGN KEY (editor) REFERENCES member(id) ON DELETE SET NULL);
CREATE TABLE note (id INT PRIMARY KEY, author INT, editor INT);
INSERT INTO member (id, name) VALUES (1, 'Eve'), (2, 'Sam');
INSERT INTO post (id, author, editor) VALUES (1, 1, 2), (2, 2, 2);
INSERT INTO post (id, author) VALUES (3, 9);
INSERT INTO post (id, author, editor) VALUES (4, 1, 9);
SELECT p.id, p.author, p.editor, m.name FROM post p JOIN member m ON p.author = m.id ORDER BY p.id;
SELECT id FROM post WHERE editor = 2 ORDER BY id;
UPDATE post SET editor = 1 WHERE id = 1;
UPDATE post SET author = 9 WHERE id = 1;
INSERT INTO note (id, author, editor) VALUES (1, 1, 2), (2, 2, 1);
ALTER TABLE note ADD CONSTRAINT note_author FOREIGN KEY (author) REFERENCES member(id) ON DELETE CASCADE;
ALTER TABLE note ADD CONSTRAINT note_editor FOREIGN KEY (editor) REFERENCES member(id);
INSERT INTO note (id, author, editor) VALUES (3, 1, 7);
SELECT id, author, editor FROM note ORDER BY id;
DELETE FROM member WHERE id = 2;
DELETE FROM member WHERE id = 1;
SELECT id, author, editor FROM post ORDER BY id;
DELETE FROM note;
DELETE FROM member WHERE id = 1;
SELECT id, author, editor FROM post ORDER BY id;
CREATE TABLE solo (id INT PRIMARY KEY, ref INT);
INSERT INTO solo (id, ref) VALUES (1, 5);
ALTER TABLE solo ADD FOREIGN KEY (ref) REFERENCES member(id);
SELECT id, ref FROM solo;
INSERT INTO post (id, author, editor) VALUES (5, 2, NULL);
UPDATE post SET editor = NULL WHERE id = 2;
INSERT INTO post (id, author, editor) VALUES (6, NULL, 2);
SELECT id, author, editor FROM post ORDER BY id;
