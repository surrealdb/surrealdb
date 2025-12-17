/**
[env]
timeout = 5000
context-timeout = 5000

[test]

[[test.results]]
value = "[{ author: 'Maxwell Flitton', id: book:1, isbn: '978-1803234694', title: 'Rust Web Programming' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: { uniq_isbn: 'DEFINE INDEX uniq_isbn ON book FIELDS isbn UNIQUE' }, lives: {  }, tables: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
match = "'status' in $result.building"

[[test.results]]
value = "{ building: { initial: 1, pending: 0, status: 'ready', updated: 0 } }"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: { ft_title: 'DEFINE INDEX ft_title ON book FIELDS title FULLTEXT ANALYZER simple BM25(1.2,0.75) HIGHLIGHTS', idx_author: 'DEFINE INDEX idx_author ON book FIELDS author', uniq_isbn: 'DEFINE INDEX uniq_isbn ON book FIELDS isbn UNIQUE' }, lives: {  }, tables: {  } }"

[[test.results]]
value = "[{ author: 'Maxwell Flitton', id: book:1, isbn: '978-1803234694', title: 'Rust Web Programming' }]"


*/
CREATE book:1 SET title = 'Rust Web Programming', isbn = '978-1803234694', author = 'Maxwell Flitton';
DEFINE INDEX uniq_isbn ON book FIELDS isbn UNIQUE;
REBUILD INDEX IF EXISTS uniq_isbn ON book;
INFO FOR TABLE book;
REBUILD INDEX IF EXISTS idx_author ON book;
REBUILD INDEX IF EXISTS ft_title ON book;
DEFINE INDEX idx_author ON book FIELDS author;
DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
DEFINE INDEX ft_title ON book FIELDS title FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;
REBUILD INDEX uniq_isbn ON book;
REBUILD INDEX idx_author ON book;
REBUILD INDEX ft_title ON book;
INFO FOR INDEX ft_title ON book;
INFO FOR INDEX ft_title ON book;
INFO FOR TABLE book;
SELECT * FROM book WHERE title @@ 'Rust';
