/**
[test]
reason = "Test DEFINE ANALYZER with various tokenizers, filters, and languages to exercise Display impls"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, analyzers: { basic: 'DEFINE ANALYZER basic TOKENIZERS BLANK,CLASS FILTERS LOWERCASE', multi_filter: 'DEFINE ANALYZER multi_filter TOKENIZERS BLANK,CAMEL,PUNCT FILTERS ASCII, LOWERCASE, UPPERCASE', ngram_analyzer: 'DEFINE ANALYZER ngram_analyzer TOKENIZERS BLANK FILTERS NGRAM(2,3)', snowball_en: 'DEFINE ANALYZER snowball_en TOKENIZERS BLANK FILTERS SNOWBALL(ENGLISH)' }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"

*/

DEFINE ANALYZER basic TOKENIZERS blank, class FILTERS lowercase;
DEFINE ANALYZER snowball_en TOKENIZERS blank FILTERS snowball(english);
DEFINE ANALYZER ngram_analyzer TOKENIZERS blank FILTERS ngram(2, 3);
DEFINE ANALYZER multi_filter TOKENIZERS blank, camel, punct FILTERS ascii, lowercase, uppercase;
INFO FOR DATABASE;
