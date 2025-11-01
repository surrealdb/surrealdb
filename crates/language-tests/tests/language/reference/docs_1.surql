/**
[test]
reason = "Example from https://surrealdb.com/docs/surrealql/datamodel/references"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ comics: [comic_book:one], id: person:mat, name: 'Mat' }]"

[[test.results]]
value = "[{ comics: [comic_book:one], id: person:nynaeve, name: 'Nynaeve' }]"

[[test.results]]
value = "[{ id: comic_book:one, title: 'Loki, God of Stories' }]"

[[test.results]]
value = "[{ id: comic_book:one, owners: [person:mat, person:nynaeve], title: 'Loki, God of Stories' }]"

[[test.results]]
value = "[{ id: comic_book:one, owners: [{ id: person:mat, name: 'Mat' }, { id: person:nynaeve, name: 'Nynaeve' }], title: 'Loki, God of Stories' }]"

[env.capabilities]
allow-experimental = ["record_references"]

*/

DEFINE FIELD comics ON person TYPE option<array<record<comic_book>>> REFERENCE;
CREATE person:mat SET 
	name = "Mat", 
	comics = [comic_book:one];
CREATE person:nynaeve SET 
	name = "Nynaeve", 
	comics = [comic_book:one];
CREATE comic_book:one SET title = "Loki, God of Stories";

SELECT 
	*, 
	<~person AS owners
FROM comic_book;

SELECT 
	*, 
	<~person.{ id, name } AS owners
FROM comic_book;