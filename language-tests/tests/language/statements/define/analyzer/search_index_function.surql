/**
[env]
timeout = 10000
context-timeout = 10000

[test]
reason = """
An analyzer's `FUNCTION` clause pre-processes every piece of text the index
stores, whichever path stored it: `blog:1` is tokenized by the index build's
initial scan, `blog:2` by the write that follows the definition. Searching for
`p` is what tells those apart from an analyzer that does nothing — it is a
token of the unstripped text and of nothing else.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: blog:1, title: '<p>hello surreal</p>' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: blog:2, title: '<p>goodbye world</p>' }]"

[[test.results]]
value = "[{ id: blog:1 }]"

[[test.results]]
value = "[{ id: blog:2 }]"

[[test.results]]
value = "[]"

*/
DEFINE FUNCTION fn::stripHtml($html: string) {
	RETURN string::replace($html, /<[^>]*>/, "");
};
DEFINE ANALYZER htmlAnalyzer FUNCTION fn::stripHtml TOKENIZERS blank,class;
CREATE blog:1 SET title = '<p>hello surreal</p>';
DEFINE INDEX blog_title ON blog FIELDS title FULLTEXT ANALYZER htmlAnalyzer BM25(1.2,0.75) HIGHLIGHTS;
CREATE blog:2 SET title = '<p>goodbye world</p>';

SELECT id FROM blog WHERE title @@ 'surreal';
SELECT id FROM blog WHERE title @@ 'world';
SELECT id FROM blog WHERE title @@ 'p';
