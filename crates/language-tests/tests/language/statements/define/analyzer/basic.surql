/**
[env]
# Extra timeout due to file reading.
timeout = 8000

[test]

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
value = '''{ accesses: {  }, analyzers: { autocomplete: 'DEFINE ANALYZER autocomplete FILTERS LOWERCASE,EDGENGRAM(2,10)', english: 'DEFINE ANALYZER english TOKENIZERS BLANK,CLASS FILTERS LOWERCASE,SNOWBALL(ENGLISH)', englishLemmatizer: 'DEFINE ANALYZER englishLemmatizer TOKENIZERS BLANK,CLASS FILTERS MAPPER(../../tests/data/lemmatization-en.txt)', htmlAnalyzer: 'DEFINE ANALYZER htmlAnalyzer FUNCTION fn::stripHtml TOKENIZERS BLANK,CLASS' }, apis: {  }, buckets: {  }, configs: {  }, functions: { stripHtml: "DEFINE FUNCTION fn::stripHtml($html: string) { RETURN string::replace($html, /<[^>]*>/, '') } PERMISSIONS FULL" }, models: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }'''

*/
DEFINE ANALYZER english TOKENIZERS blank,class FILTERS lowercase,snowball(english);
DEFINE ANALYZER autocomplete FILTERS lowercase,edgengram(2,10);
DEFINE FUNCTION fn::stripHtml($html: string) {
	RETURN string::replace($html, /<[^>]*>/, "");
};
DEFINE ANALYZER htmlAnalyzer FUNCTION fn::stripHtml TOKENIZERS blank,class;
DEFINE ANALYZER englishLemmatizer TOKENIZERS blank,class FILTERS mapper('../../tests/data/lemmatization-en.txt');
INFO FOR DB;
