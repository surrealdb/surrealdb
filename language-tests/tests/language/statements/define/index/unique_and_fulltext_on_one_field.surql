/**
[test]
reason = "A field that is both unique and full-text searchable takes two indexes, one of each kind, and both are enforced"
issue = 1495

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: { title_search: 'DEFINE INDEX title_search ON book FIELDS title FULLTEXT ANALYZER simple BM25(1.2,0.75)', title_unique: 'DEFINE INDEX title_unique ON book FIELDS title UNIQUE' }, lives: {  }, tables: {  } }"

[[test.results]]
value = "[{ id: book:1, title: 'Rust in Action' }]"

[[test.results]]
error = "Database index `title_unique` already contains 'Rust in Action', with record `book:1`"

[[test.results]]
value = "[{ id: book:1 }]"

*/

DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;
DEFINE INDEX title_unique ON book FIELDS title UNIQUE;
DEFINE INDEX title_search ON book FIELDS title FULLTEXT ANALYZER simple BM25;
INFO FOR TABLE book;
CREATE book:1 SET title = 'Rust in Action';
CREATE book:2 SET title = 'Rust in Action';
SELECT id FROM book WHERE title @@ 'rust';
