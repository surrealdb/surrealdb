/**
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
value = "[{ id: account:2jf4dlsl6ekz0okxuodl, name: 'Tobie', user_defined_id: 'Tobie' }]"
skip-record-id-key = true

*/
DEFINE ANALYZER name TOKENIZERS class FILTERS lowercase,ngram(1,128);
DEFINE ANALYZER userdefinedid TOKENIZERS blank FILTERS lowercase,ngram(1,32);
DEFINE INDEX account_name_search_idx ON TABLE account COLUMNS name SEARCH ANALYZER name BM25(1.2,0.75) HIGHLIGHTS;
DEFINE INDEX account_user_defined_id_search_idx ON TABLE account COLUMNS user_defined_id SEARCH ANALYZER userdefinedid BM25 HIGHLIGHTS;
CREATE account SET name='Tobie', user_defined_id='Tobie';
