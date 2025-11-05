/**
[test]
reason = "Example from https://surrealdb.com/docs/surrealql/datamodel/references"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ comics: [comic_book:one], id: person:one }, { comics: [comic_book:one], id: person:two }]"

[[test.results]]
value = "[{ id: publisher:one, products: [comic_book:one, book:one] }]"

[[test.results]]
value = "[{ id: comic_book:one, owners: [person:one, person:two], publishers: [publisher:one], title: 'Loki, God of Stories' }]"

[[test.results]]
value = "[{ id: comic_book:one, owners: [person:one, person:two], publishers: [publisher:one], title: 'Loki, God of Stories' }]"

*/

DEFINE FIELD comics ON person TYPE option<array<record<comic_book>>> REFERENCE;
DEFINE FIELD products ON publisher TYPE option<array<record<comic_book|book>>> REFERENCE;
DEFINE FIELD owners ON comic_book COMPUTED <~person;
DEFINE FIELD publishers ON comic_book COMPUTED <~publisher;

CREATE person:one, person:two SET comics = [comic_book:one];
CREATE publisher:one SET products = [comic_book:one, book:one];
CREATE comic_book:one SET title = "Loki, God of Stories";
SELECT * FROM comic_book;