/**
[env]
imports = ["upgrade/define/3.00.00_index_fulltext_import.surql"]

[test]
upgrade = true
reason = """
A FULLTEXT index written by an older binary keeps its own doc-ID space until it
is explicitly rebuilt, so MATCHES, BM25 scoring and highlights must return the
same rows on the new binary reading the posting lists the old one wrote.

The post-upgrade write lives in 3.00.00_index_fulltext_current.surql: a body carrying a
`[[test.repair]]` is run twice, so it cannot mutate the state it reads.

Releases in the 3.3.0-beta line are the exception and say so through
`[[test.repair]]`: they reject an index stamped with the pre-shared-doc-ID
format instead of reading it, so an operator has to rebuild. Declaring that
keeps it checkable in both directions — the runner requires the damage to
actually happen on those targets, and fails the moment it stops."""

[[test.repair]]
since = ">=3.3.0-beta.1, <3.4.0"
script = "upgrade/define/3.00.00_index_fulltext_rebuild.surql"
unrepaired = "broken"
reason = """
The 3.3.0 betas moved full-text indexes onto a table-level shared doc-ID space
and refuse to serve an index still stamped with the per-index format, so
`REBUILD INDEX` is a required manual step when upgrading onto one of them."""

[[test.results]]
match = '''$result.indexes.blog_title == "DEFINE INDEX blog_title ON blog FIELDS title FULLTEXT ANALYZER simple BM25(1.2,0.75) HIGHLIGHTS"'''

[[test.results]]
value = "[{ id: blog:1, label: 'test', title: 'the quick brown fox jumped over the lazy <em>dog</em>' }, { id: blog:2, label: 'test', title: 'the fast fox jumped over the lazy <em>dog</em>' }, { id: blog:4, label: 'test', title: 'the <em>dog</em> sat there and did nothing' }]"

[[test.results]]
value = "[]"

*/
INFO FOR TABLE blog;
-- Read the posting lists the OLD binary wrote, with no repair in between.
-- Three of four blogs mention "dog" (blog:1, blog:2, blog:4); blog:3 is about
-- animals/watching.
SELECT id, label, search::highlight('<em>', '</em>', 1) AS title FROM blog
    WHERE title @1@ 'dog' ORDER BY id;
-- A non-matching term returns empty.
SELECT id FROM blog WHERE title @1@ 'zebra';
