/**
[test]

[[test.results]]
value = "NONE"

*/
DEFINE ANALYZER snow_ana TOKENIZERS blank, class FILTERS lowercase, snowball(english);
