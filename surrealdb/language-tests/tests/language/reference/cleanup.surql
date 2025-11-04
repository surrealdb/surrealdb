/**

[env.capabilities]
allow-experimental = ["record_references"]

[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[comic_book:one, comic_book:two]"

[[test.results]]
value = "[person:one, person:two]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[comic_book:two]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[person:two]"


*/
DEFINE TABLE person;
DEFINE FIELD comics ON person TYPE option<array<record<comic_book>>> REFERENCE ON DELETE UNSET;
CREATE person:one, person:two SET comics = [comic_book:one, comic_book:two] RETURN NONE;

DEFINE TABLE comic_book;
DEFINE FIELD owners ON comic_book COMPUTED <~person;
CREATE comic_book:one, comic_book:two RETURN NONE;

RETURN person:one.comics;
RETURN comic_book:two.owners;

DELETE comic_book:one;
RETURN person:one.comics;

DELETE person:one;
RETURN comic_book:two.owners;
