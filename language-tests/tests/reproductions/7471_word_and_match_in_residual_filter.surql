/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A word-AND full-text match must require every term when it is evaluated as a residual filter, not only when it drives an index scan"
issue = 7471

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: article:a, title: 'memory safety guide' }]"

[[test.results]]
value = "[{ id: article:b, title: 'memory alone' }]"

[[test.results]]
value = "['a']"

[[test.results]]
value = "['a']"

[[test.results]]
value = "['a']"
*/

DEFINE ANALYZER nail_fts TOKENIZERS blank, class, camel, punct FILTERS ascii, lowercase;
DEFINE TABLE article SCHEMALESS;
DEFINE INDEX title_fts ON TABLE article FIELDS title FULLTEXT ANALYZER nail_fts;
CREATE article:a SET title = 'memory safety guide';
CREATE article:b SET title = 'memory alone';
SELECT VALUE meta::id(id) FROM article WHERE title @1@ 'memory safety';
SELECT VALUE meta::id(id) FROM article WHERE title @1@ 'memory safety' OR meta::id(id) = 'article:zzz';
SELECT VALUE meta::id(id) FROM article WITH NOINDEX WHERE title @1@ 'memory safety';
