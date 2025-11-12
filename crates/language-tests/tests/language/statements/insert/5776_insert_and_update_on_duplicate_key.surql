/**
[env]
clean = true

[test]
reason = "Ensure a transaction fully rolls back if a unique constraint on 'name' is violated by a subsequent INSERT into 'item' within the same transaction. No data should persist."
issue = 5776
run = true

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ author: 'John Doe', content: 'This is the first version of the article content.', feed_title: 'Tech News', fetched_full: false, id: article:hb57sw2anrqfoqgpp56o, link: 'https://example.com/news/123', name: 'Breaking News Article', read: false, starred: false }]"
skip-record-id-key = true

[[test.results]]
value = "[{ author: 'Jane Smith', content: 'This is a completely different article content with updated information.', feed_title: 'Science News', fetched_full: true, id: article:hb57sw2anrqfoqgpp56o, link: 'https://example.com/news/123', name: 'Breaking News Article', read: true, starred: true }]"
skip-record-id-key = true

[[test.results]]
value = "NONE"

[[test.results]]
# Statement 6: SELECT * FROM article;
value = "[{ author: 'Jane Smith', content: 'This is a completely different article content with updated information.', feed_title: 'Science News', fetched_full: true, id: article:54d6j987q3orc52vpjvb, link: 'https://example.com/news/123', name: 'Breaking News Article', read: true, starred: true }]"
skip-record-id-key = true
*/

DEFINE INDEX article_name_link_index ON TABLE article COLUMNS name, link UNIQUE;

BEGIN;
INSERT INTO article {
    name: "Breaking News Article",
    link: "https://example.com/news/123",
    content: "This is the first version of the article content.",
    read: false,
    starred: false,
    feed_title: "Tech News",
    author: "John Doe",
    fetched_full: false
};

INSERT INTO article {
    name: "Breaking News Article",
    link: "https://example.com/news/123",
    content: "This is a completely different article content with updated information.",
    read: true,
    starred: true,
    feed_title: "Science News",
    author: "Jane Smith",
    fetched_full: true
} ON DUPLICATE KEY
UPDATE content = "This is a completely different article content with updated information.",
    read = true,
    starred = true,
    feed_title = "Science News",
    author = "Jane Smith",
    fetched_full = true;
COMMIT;

SELECT * FROM article;
