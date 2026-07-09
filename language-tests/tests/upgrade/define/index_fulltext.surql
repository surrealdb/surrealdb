/**
[env]
imports = ["upgrade/define/index_fulltext_import.surql"]

[test]
upgrade = true
reason = "A FULLTEXT index built by an older version uses the retired per-index doc-ID format and must be rebuilt after upgrade onto the shared table-level doc-ID space; REBUILD INDEX restores it and BM25 highlights query correctly"

[[test.results]]
match = '''$result.indexes.blog_title == "DEFINE INDEX blog_title ON blog FIELDS title FULLTEXT ANALYZER simple BM25(1.2,0.75) HIGHLIGHTS"'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: blog:1, label: 'test', title: 'the quick brown fox jumped over the lazy <em>dog</em>' }, { id: blog:2, label: 'test', title: 'the fast fox jumped over the lazy <em>dog</em>' }, { id: blog:4, label: 'test', title: 'the <em>dog</em> sat there and did nothing' }]"

[[test.results]]
value = "[]"

*/
INFO FOR TABLE blog;
-- A FULLTEXT index built before the shared table-level doc-ID space must be
-- rebuilt after upgrade before it can be queried.
REBUILD INDEX blog_title ON blog;
-- Three of four blogs mention "dog" (blog:1, blog:2, blog:4); blog:3 is about animals/watching.
SELECT id, label, search::highlight('<em>', '</em>', 1) AS title FROM blog
    WHERE title @1@ 'dog' ORDER BY id;
-- A non-matching term returns empty.
SELECT id FROM blog WHERE title @1@ 'zebra';
