/**
[env]
imports = ["upgrade/define/analyzer_ngram_import.surql"]

[test]
upgrade = true
reason = "Analyzer with ngram filter survives upgrade — definition string round-trips and search::analyze produces the expected token stream"

[[test.results]]
match = '''$result.analyzers.ngram_ana == "DEFINE ANALYZER ngram_ana TOKENIZERS CLASS FILTERS LOWERCASE, NGRAM(2,4)"'''

[[test.results]]
value = "['fo', 'fox', 'ox']"

*/
INFO FOR DB;
search::analyze('ngram_ana', 'Fox');
