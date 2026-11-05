/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A field read through a COMPUTED reverse reference returns the fields of the referencing records, the same values a filter on the referencing field returns, with or without an index on it."
issue = 6616

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: users:1, name: 'a', posts: [] }]"

[[test.results]]
value = "[{ id: users:2, name: 'b', posts: [] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['b1', 'b2', 'b4', 'b5', 'b7', 'b8']"

[[test.results]]
value = "['b1', 'b2', 'b4', 'b5', 'b7', 'b8']"

[[test.results]]
value = "['b1', 'b2', 'b4', 'b5', 'b7', 'b8']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['b1', 'b2', 'b4', 'b5', 'b7', 'b8']"

[[test.results]]
value = "{ attributes: { expr: 'body' }, children: [{ attributes: { access: '= users:1', direction: 'Forward', index: 'author_idx', table: 'posts' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'ProjectValue' }"
*/

DEFINE FIELD author ON posts TYPE option<record<users>> REFERENCE;
DEFINE FIELD posts ON users COMPUTED <~(posts FIELD author);
CREATE users:1 SET name = 'a';
CREATE users:2 SET name = 'b';
FOR $i IN 0..10 { CREATE type::record('posts', $i) SET author = IF $i % 3 = 0 { users:2 } ELSE { users:1 }, body = 'b' + <string>$i; };

RETURN array::sort((SELECT VALUE posts.body FROM ONLY users:1));
RETURN array::sort((users:1<~posts).body);
RETURN array::sort(SELECT VALUE body FROM posts WHERE author = users:1);
DEFINE INDEX author_idx ON TABLE posts COLUMNS author;
RETURN array::sort(SELECT VALUE body FROM posts WHERE author = users:1);
SELECT VALUE body FROM posts WHERE author = users:1 EXPLAIN;
