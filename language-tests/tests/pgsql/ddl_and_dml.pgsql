/**
[env]
namespace = true
database = true

[test]
reason = "PostgreSQL CREATE TABLE, INSERT, and SELECT pipeline"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: tags:1, tag: 'rust' }, { id: tags:2, tag: 'database' }]"

[[test.results]]
value = "[{ tag: 'database' }, { tag: 'rust' }]"
*/

CREATE TABLE tags (id INT, tag VARCHAR(50));
INSERT INTO tags (id, tag) VALUES (1, 'rust'), (2, 'database');
SELECT t.tag FROM tags t ORDER BY t.tag ASC;
