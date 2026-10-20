/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "search::score(n) reports the score of the n-th MATCHES clause for a row whichever branch of an OR matched"
issue = 7112

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: article:1, text: 'IN the year 1878 I took my degree of Doctor of Medicine', title: 'Sherlock Holmes - A Study in Scarlet' }, { id: article:2, text: 'SHERLOCK HOLMES took his bottle from the corner', title: 'Sherlock Holmes - The Sign of the Four' }, { id: article:3, text: 'MR. SHERLOCK HOLMES, who was usually very late', title: 'The Hound' }]"

[[test.results]]
value = "[{ id: 1, s0: true, s1: true }]"

[[test.results]]
value = "[{ id: 1, s0: false, s1: true }, { id: 2, s0: true, s1: false }, { id: 3, s0: true, s1: false }]"

[[test.results]]
value = "[{ id: 1, s0: true, s1: false }, { id: 2, s0: true, s1: true }, { id: 3, s0: false, s1: true }]"
*/

DEFINE ANALYZER my_analyzer TOKENIZERS class, blank FILTERS lowercase, ascii;
DEFINE TABLE article;
DEFINE INDEX text_index ON TABLE article FIELDS text FULLTEXT ANALYZER my_analyzer BM25 HIGHLIGHTS;
DEFINE INDEX title_index ON TABLE article FIELDS title FULLTEXT ANALYZER my_analyzer BM25 HIGHLIGHTS;
INSERT INTO article (id, title, text) VALUES
 (1, "Sherlock Holmes - A Study in Scarlet", "IN the year 1878 I took my degree of Doctor of Medicine"),
 (2, "Sherlock Holmes - The Sign of the Four", "SHERLOCK HOLMES took his bottle from the corner"),
 (3, "The Hound", "MR. SHERLOCK HOLMES, who was usually very late");
SELECT meta::id(id) AS id, search::score(0) > 0 AS s0, search::score(1) > 0 AS s1 FROM article WHERE text @0@ "medicine" OR title @1@ "scarlet" ORDER BY id;
SELECT meta::id(id) AS id, search::score(0) > 0 AS s0, search::score(1) > 0 AS s1 FROM article WHERE text @0@ "sherlock" OR title @1@ "scarlet" ORDER BY id;
SELECT meta::id(id) AS id, search::score(0) > 0 AS s0, search::score(1) > 0 AS s1 FROM article WHERE title @0@ "sherlock" OR text @1@ "sherlock" ORDER BY id;
