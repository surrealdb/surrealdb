/**
[env]
timeout = 10000

[test]

[[test.results]]
value = "[{ id: blog:1, title: 'Understanding SurrealQL and how it is different from PostgreSQL' }]"

[[test.results]]
value = "[{ id: blog:3, title: 'This blog is going to be deleted' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: blog:2, title: 'Behind the scenes of the exciting beta 9 release' }]"

[[test.results]]
value = "[]"


[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: { blog_title: 'DEFINE INDEX blog_title ON blog FIELDS title SEARCH ANALYZER simple BM25(1.2,0.75) DOC_IDS_ORDER 100 DOC_LENGTHS_ORDER 100 POSTINGS_ORDER 100 TERMS_ORDER 100 DOC_IDS_CACHE 100 DOC_LENGTHS_CACHE 100 POSTINGS_CACHE 100 TERMS_CACHE 100 HIGHLIGHTS' }, lives: {  }, tables: {  } }"

[[test.results]]
match= """
// $result.postings.total_size is inconsistent so don't match it.

$result.doc_ids == { keys_count: 2, max_depth: 1, nodes_count: 1, total_size: 63 }
 && $result.doc_lengths == { keys_count: 2, max_depth: 1, nodes_count: 1, total_size: 56 }
 && $result.terms == { keys_count: 17, max_depth: 1, nodes_count: 1, total_size: 199 }
 && $result.postings.keys_count == 17
 && $result.postings.max_depth == 1
 && $result.postings.nodes_count == 1
"""

*/
CREATE blog:1 SET title = 'Understanding SurrealQL and how it is different from PostgreSQL';
CREATE blog:3 SET title = 'This blog is going to be deleted';
DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
DEFINE INDEX blog_title ON blog FIELDS title SEARCH ANALYZER simple BM25(1.2,0.75) HIGHLIGHTS;
CREATE blog:2 SET title = 'Behind the scenes of the exciting beta 9 release';
DELETE blog:3;

INFO FOR TABLE blog;
ANALYZE INDEX blog_title ON blog;
