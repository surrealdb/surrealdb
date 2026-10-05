/**
[env]
timeout = 5000
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A ranked full-text OR over records carrying a wide field returns every match when the matches span several resolve batches"
issue = 7383

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
value = "300"

[[test.results]]
value = "1200"
*/

DEFINE ANALYZER english TOKENIZERS blank, class, punct FILTERS lowercase, ascii, snowball(english);
DEFINE TABLE doc SCHEMALESS;
DEFINE INDEX doc_grp ON doc FIELDS grp;
DEFINE INDEX doc_body ON doc FIELDS body FULLTEXT ANALYZER english BM25 HIGHLIGHTS;
FOR $i IN 0..1200 {
	CREATE doc SET grp = 'big', body = 'student report ' + <string> $i, emb = array::repeat(0.5f, 64);
};
RETURN array::len(
	SELECT id, search::score(1) + search::score(2) AS score FROM doc
	WHERE grp = 'big' AND (body @1@ 'student' OR body @2@ 'report')
	ORDER BY score DESC LIMIT 300
);
RETURN array::len(
	SELECT id FROM doc WHERE grp = 'big' AND (body @1@ 'student' OR body @2@ 'report')
);
