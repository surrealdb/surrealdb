/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Incorrect arguments for function fn::unsupportedFunction(). The function expects 0 arguments."

*/
DEFINE FUNCTION fn::unsupportedFunction() {
	RETURN 1;
};
DEFINE ANALYZER htmlAnalyzer FUNCTION fn::unsupportedFunction TOKENIZERS blank,class;
RETURN search::analyze('htmlAnalyzer', '<p>This is a <em>sample</em> of HTML</p>');
