/**
[test]
introduced = ">=3.3.0-beta.5"
reason = """
Setup for 3.03.00_doc_ids_numeric_twins: two records whose ids differ only in a
nested number's kind, and a third, mapped in the table's shared doc-ID space by
a full-text index. A 3.3 release writes each mapping under the injective `!dj`,
the reverse `!dd`, and the type-erased `!di`, which the twins share."""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ body: 'apple', id: t:[1] }]"

[[test.results]]
value = "[{ body: 'banana', id: t:[1dec] }]"

[[test.results]]
value = "[{ body: 'apple cherry', id: t:[2] }]"

*/
DEFINE ANALYZER simple TOKENIZERS blank, class FILTERS lowercase;
DEFINE INDEX t_body ON t FIELDS body FULLTEXT ANALYZER simple BM25;
CREATE t:[1] SET body = 'apple';
CREATE t:[1dec] SET body = 'banana';
CREATE t:[2] SET body = 'apple cherry';
