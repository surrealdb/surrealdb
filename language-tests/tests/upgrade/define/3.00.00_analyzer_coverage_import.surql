/**
[test]
reason = "Setup for analyzer_coverage.surql: the old binary stores analyzer definitions built from the tokenizers and filters no other upgrade test writes."
run = false
*/
DEFINE ANALYZER up_analyzer_coverage_camel TOKENIZERS camel FILTERS lowercase;
DEFINE ANALYZER up_analyzer_coverage_punct TOKENIZERS punct FILTERS uppercase;
DEFINE ANALYZER up_analyzer_coverage_ascii TOKENIZERS blank FILTERS ascii;
DEFINE ANALYZER up_analyzer_coverage_edge TOKENIZERS class FILTERS lowercase, edgengram(2, 4);
DEFINE ANALYZER up_analyzer_coverage_mixed TOKENIZERS camel, punct FILTERS ascii, uppercase;
