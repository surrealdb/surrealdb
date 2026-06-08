/**
[env]
imports = ["upgrade/define/analyzer_snowball_import.surql"]

[test]
upgrade = true
reason = "Analyzer with snowball english stemmer filter survives upgrade — stems are produced identically post-upgrade"

[[test.results]]
match = '''$result.analyzers.snow_ana == "DEFINE ANALYZER snow_ana TOKENIZERS BLANK,CLASS FILTERS LOWERCASE, SNOWBALL(ENGLISH)"'''

[[test.results]]
value = "['run', 'jump', 'quick']"

*/
INFO FOR DB;
search::analyze('snow_ana', 'running jumped quickly');
