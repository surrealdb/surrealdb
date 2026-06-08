/**
[env]
imports = ["upgrade/define/field_reference_import.surql"]

[test]
upgrade = true
reason = "REFERENCE field + COMPUTED back-traversal survive upgrade — exercises the Ref key family (back-link tracking). Alice has 2 books, Bob has 1; the COMPUTED <~book field resolves the back-pointer after the to-version reads the on-disk references."

[[test.results]]
match = '''string::contains($result.fields.author, "REFERENCE ON DELETE UNSET")'''

[[test.results]]
match = '''string::contains($result.fields.books, "COMPUTED <~book")'''

[[test.results]]
value = "[{ books: [book:1, book:2], id: author:a, name: 'Alice' }, { books: [book:3], id: author:b, name: 'Bob' }]"

*/
INFO FOR TABLE book;
INFO FOR TABLE author;
SELECT id, name, books FROM author ORDER BY id;
