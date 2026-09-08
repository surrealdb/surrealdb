/**
[test]

[[test.results]]
value = "NONE"

*/
DEFINE ANALYZER ngram_ana TOKENIZERS class FILTERS lowercase, ngram(2, 4);
