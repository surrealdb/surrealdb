/**
[env]
trace = ["links"]

[test]
reason = "A record link the result reads through is reported as [record, field, target]; one only returned, or read in a condition, is not"

[[test.results]]
value = "[{ id: org:sdb, name: 'SurrealDB' }]"

[[test.results]]
value = "[{ id: person:tobie, name: 'Tobie', org: org:sdb }]"

[[test.results]]
value = "[{ id: tag:rust, label: 'rust' }]"

[[test.results]]
value = "[{ id: tag:db, label: 'db' }]"

[[test.results]]
value = "[{ id: post:1 }]"

[[test.results]]
value = "[{ author: person:tobie, id: post:2, title: 'World' }]"

[[test.results]]
value = "[{ author: { name: 'Tobie' }, title: 'Hello' }]"
trace = "{ links: [[post:1, 'author', person:tobie]] }"

[[test.results]]
value = "[['rust', 'db']]"
trace = "{ links: [[post:1, 'tags', tag:db], [post:1, 'tags', tag:rust]] }"

[[test.results]]
value = "['SurrealDB']"
trace = "{ links: [[person:tobie, 'org', org:sdb], [post:1, 'author', person:tobie]] }"

[[test.results]]
value = "[{ first: 'rust' }]"
trace = "{ links: [[post:1, 'tags', tag:rust]] }"

[[test.results]]
value = "[{'rust',}]"
trace = "{ links: [[post:1, 'labels', tag:rust]] }"

[[test.results]]
value = "[{ author: person:tobie }]"

[[test.results]]
value = "[{ title: 'Hello' }, { title: 'World' }]"

[[test.results]]
value = "[{ name: NONE }]"
trace = "{ links: [[post:1, 'missing', person:nobody]] }"

[[test.results]]
value = "['Tobie', 'Tobie']"
trace = "{ links: [[post:1, 'author', person:tobie], [post:2, 'author', person:tobie]] }"

[[test.results]]
value = "'Tobie'"
trace = "{ partial: true }"

[[test.results]]
value = "'Tobie'"
trace = "{ partial: true }"

[[test.results]]
value = "[{ name: 'Tobie' }]"
trace = "{ links: [[post:1, 'author', person:tobie]] }"

[[test.results]]
value = "[{ title: 'Hello' }]"

[[test.results]]
value = "[{ author: { name: 'Tobie' } }]"
trace = "{ partial: true }"

[[test.results]]
value = "['TOBIE']"
trace = "{ partial: true }"

[[test.results]]
value = "[5]"
*/

-- 0: setup
CREATE org:sdb SET name = 'SurrealDB';
CREATE person:tobie SET name = 'Tobie', org = org:sdb;
CREATE tag:rust SET label = 'rust';
CREATE tag:db SET label = 'db';
CREATE post:1 SET title = 'Hello', author = person:tobie, tags = [tag:rust, tag:db], labels = <set> [tag:rust], missing = person:nobody RETURN id;
CREATE post:2 SET title = 'World', author = person:tobie;

-- 6: a field read through a link
SELECT title, author.name FROM post:1;

-- 7: every element of an array of links
SELECT VALUE tags.*.label FROM post:1;

-- 8: a chain of links reports each one
SELECT VALUE author.org.name FROM post:1;

-- 9: an element of an array of links, by index
SELECT tags[0].label AS first FROM post:1;

-- 10: a set of links
SELECT VALUE labels.*.label FROM post:1;

-- 11: a link only returned is not read through
SELECT author FROM post:1;

-- 12: a link read in a condition is not traced
SELECT title FROM post WHERE author.name = 'Tobie';

-- 13: a link to a missing record: the field's value is the id
SELECT missing.name AS name FROM post:1;

-- 14: the same link read from two rows is reported for each row
SELECT VALUE author.name FROM post;

-- 15: a holder only a subquery names is not evaluated twice: partial
RETURN (SELECT * FROM ONLY post:1).author.name;

-- 16: link reads a step cuts cannot be pruned: partial
RETURN [post:1.author.name, post:2.author.name][0];

-- 17: a destructure reads through the link
SELECT VALUE author.{ name } FROM post:1;

-- 18: a field with no link is named as it is untraced, after its constant gate folds
SELECT true AND title FROM post:1;

-- 19: an operator over a link read keeps the field's name, and marks the trace partial
SELECT true AND author.name FROM post:1;

-- 20: so does a function over a link read
SELECT VALUE string::uppercase(author.name) FROM post:1;

-- 21: a function over a field that is no link reads through nothing: no trace
SELECT VALUE string::len(title) FROM post:1;
