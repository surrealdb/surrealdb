/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ books: [], id: author:a, name: 'Alice' }]"

[[test.results]]
value = "[{ books: [], id: author:b, name: 'Bob' }]"

[[test.results]]
value = "[{ author: author:a, id: book:1, title: 'X' }]"

[[test.results]]
value = "[{ author: author:a, id: book:2, title: 'Y' }]"

[[test.results]]
value = "[{ author: author:b, id: book:3, title: 'Z' }]"

*/
DEFINE TABLE author SCHEMALESS;
DEFINE TABLE book SCHEMALESS;
DEFINE FIELD author ON book TYPE record<author> REFERENCE ON DELETE UNSET;
DEFINE FIELD books ON author COMPUTED <~book;
CREATE author:a SET name = 'Alice';
CREATE author:b SET name = 'Bob';
CREATE book:1 SET title = 'X', author = author:a;
CREATE book:2 SET title = 'Y', author = author:a;
CREATE book:3 SET title = 'Z', author = author:b;
